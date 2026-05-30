"""
Bully Algorithm for Leader Election.

How it works:
- Each node has a unique NODE_ID (integer). Higher ID = higher priority.
- A node starts an election when it notices the leader is missing.
- It sends ELECTION messages to all nodes with higher IDs.
- If no higher node responds with OK \u2192 this node wins and broadcasts COORDINATOR.
- Any node that receives ELECTION from a lower-ID node replies OK and starts its own election.
"""

import os
import threading
import time
import logging
import requests

logger = logging.getLogger(__name__)

NODE_ID = int(os.environ.get("NODE_ID", 1))
PEERS_ENV = os.environ.get("PEERS", "")          # "http://node-2:8080,http://node-3:8080"
ELECTION_TIMEOUT = float(os.environ.get("ELECTION_TIMEOUT", 3))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", 5))

# ---------- shared state ----------
state = {
    "leader_id": None,
    "leader_url": None,
    "election_in_progress": False,
    "running": True,
}
state_lock = threading.Lock()


def get_peers() -> list[dict]:
    """Parse PEERS env var into a list of {id, url} dicts."""
    peers = []
    for entry in PEERS_ENV.split(","):
        entry = entry.strip()
        if not entry:
            continue
        # Expected format: "http://node-2:8080" \u2014 extract ID from hostname
        try:
            host = entry.split("//")[-1].split(":")[0]   # e.g. "node-2"
            peer_id = int(host.split("-")[-1])
            peers.append({"id": peer_id, "url": entry})
        except (ValueError, IndexError):
            logger.warning("Could not parse peer entry: %s", entry)
    return peers


def higher_peers() -> list[dict]:
    return [p for p in get_peers() if p["id"] > NODE_ID]


def all_peers() -> list[dict]:
    return get_peers()


def _post(url: str, path: str, data: dict, timeout: float = ELECTION_TIMEOUT) -> bool:
    """POST to a peer; return True on success."""
    try:
        r = requests.post(f"{url}{path}", json=data, timeout=timeout)
        return r.status_code < 500
    except requests.RequestException:
        return False


# ---------- public API ----------

def start_election():
    """Initiate a Bully election from this node."""
    with state_lock:
        if state["election_in_progress"]:
            return
        state["election_in_progress"] = True
        state["leader_id"] = None
        state["leader_url"] = None

    logger.info("[Node %d] Starting election", NODE_ID)
    higher = higher_peers()

    if not higher:
        # No higher nodes \u2014 we win immediately
        logger.info("[Node %d] No higher peers, declaring victory", NODE_ID)
        _finish_election_as_winner()
        return

    # Send ELECTION to all higher-ID nodes
    got_ok = False
    for peer in higher:
        ok = _post(peer["url"], "/election/election", {"sender_id": NODE_ID})
        if ok:
            got_ok = True

    if not got_ok:
        # No one responded \u2014 we win
        logger.info("[Node %d] No OK received, declaring victory", NODE_ID)
        _finish_election_as_winner()
    else:
        # Wait for a COORDINATOR message; if none arrives, restart
        threading.Timer(ELECTION_TIMEOUT * 2, _check_coordinator_received).start()


def handle_election_message(sender_id: int):
    """
    Called when we receive an ELECTION message from sender_id.
    We reply OK and start our own election if not already running.
    """
    logger.info("[Node %d] Received ELECTION from %d", NODE_ID, sender_id)
    # The HTTP response itself serves as the OK reply (see router below)
    threading.Thread(target=start_election, daemon=True).start()


def declare_victory():
    """Announce self as leader to all peers."""
    _finish_election_as_winner()


def _finish_election_as_winner():
    my_url = _self_url()
    with state_lock:
        state["leader_id"] = NODE_ID
        state["leader_url"] = my_url
        state["election_in_progress"] = False

    logger.info("[Node %d] I am the new leader", NODE_ID)

    for peer in all_peers():
        _post(peer["url"], "/election/coordinator", {
            "leader_id": NODE_ID,
            "leader_url": my_url,
        })


def handle_coordinator_message(leader_id: int, leader_url: str):
    """Called when we receive a COORDINATOR message."""
    logger.info("[Node %d] New leader is %d (%s)", NODE_ID, leader_id, leader_url)
    with state_lock:
        state["leader_id"] = leader_id
        state["leader_url"] = leader_url
        state["election_in_progress"] = False


def heartbeat_check():
    """
    Background thread: periodically ping the leader.
    If it's unreachable, start a new election.
    """
    # Give the cluster time to boot before the first check
    time.sleep(HEARTBEAT_INTERVAL * 2)

    while state["running"]:
        time.sleep(HEARTBEAT_INTERVAL)
        with state_lock:
            leader_url = state["leader_url"]
            leader_id = state["leader_id"]
            in_progress = state["election_in_progress"]

        if in_progress:
            continue

        if leader_id is None or leader_url is None:
            logger.info("[Node %d] No leader known, starting election", NODE_ID)
            threading.Thread(target=start_election, daemon=True).start()
            continue

        if leader_id == NODE_ID:
            continue  # We are the leader

        try:
            r = requests.get(f"{leader_url}/election/status", timeout=ELECTION_TIMEOUT)
            if r.status_code != 200:
                raise requests.RequestException("bad status")
        except requests.RequestException:
            logger.warning("[Node %d] Leader %d unreachable, starting election",
                           NODE_ID, leader_id)
            with state_lock:
                state["leader_id"] = None
                state["leader_url"] = None
            threading.Thread(target=start_election, daemon=True).start()


def _check_coordinator_received():
    with state_lock:
        leader = state["leader_id"]
        in_progress = state["election_in_progress"]
    if in_progress and leader is None:
        logger.info("[Node %d] No COORDINATOR received after timeout, re-electing", NODE_ID)
        with state_lock:
            state["election_in_progress"] = False
        threading.Thread(target=start_election, daemon=True).start()


def _self_url() -> str:
    port = os.environ.get("PORT", "8080")
    return f"http://node-{NODE_ID}:{port}"


def get_status() -> dict:
    with state_lock:
        return {
            "node_id": NODE_ID,
            "leader_id": state["leader_id"],
            "leader_url": state["leader_url"],
            "election_in_progress": state["election_in_progress"],
        }


def start_background_heartbeat():
    t = threading.Thread(target=heartbeat_check, daemon=True)
    t.start()

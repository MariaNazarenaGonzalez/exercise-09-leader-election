from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate
from src import election
from pydantic import BaseModel

from sqlalchemy.exc import ProgrammingError, IntegrityError
try:
    Base.metadata.create_all(bind=engine)
except (ProgrammingError, IntegrityError):
    pass  # Otro nodo ya creó la tabla — está bien
app = FastAPI()

# ── Start heartbeat background thread on startup ──────────────────────────────
@app.on_event("startup")
def on_startup():
    election.start_background_heartbeat()

# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}

# ── Node CRUD ──────────────────────────────────────────────────────────────────
@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node

@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()

@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node

@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node

@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)

# ── Election endpoints (Bully protocol messages) ───────────────────────────────
class ElectionMsg(BaseModel):
    sender_id: int

class CoordinatorMsg(BaseModel):
    leader_id: int
    leader_url: str

@app.post("/election/election", status_code=200)
def receive_election(msg: ElectionMsg):
    """Receive an ELECTION message — reply 200 (= OK) and start own election."""
    election.handle_election_message(msg.sender_id)
    return {"ok": True}

@app.post("/election/coordinator", status_code=200)
def receive_coordinator(msg: CoordinatorMsg):
    """Receive a COORDINATOR (victory) message."""
    election.handle_coordinator_message(msg.leader_id, msg.leader_url)
    return {"ok": True}

@app.get("/election/status")
def election_status():
    """Heartbeat + election state — used by peers to check if leader is alive."""
    return election.get_status()

@app.post("/election/start", status_code=202)
def trigger_election():
    """Manually trigger an election (useful for testing)."""
    import threading
    threading.Thread(target=election.start_election, daemon=True).start()
    return {"message": "election started"}
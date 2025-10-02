import os
from fastapi import FastAPI, Body, Query
from pydantic import BaseModel
from agents.router import Router

app = FastAPI(title="StoreBot (Agno + Ollama)")

router = Router()

class ChatIn(BaseModel):
    message: str
    confirmed: bool | None = False
    sender_email: str | None = "user@example.com"
    region: str | None = None
    state: str | None = None
    segment: str | None = None
    category: str | None = None
    send_email: bool | None = False
    to: str | None = None

@app.post("/chat")
def chat(inp: ChatIn):
    out = router.handle(
        inp.message,
        confirmed=inp.confirmed,
        sender_email=inp.sender_email,
        region=inp.region, state=inp.state, segment=inp.segment, category=inp.category,
        send_email=inp.send_email, to=inp.to
    )
    return {"reply": out}

@app.get("/")
def health():
    return {"ok": True}


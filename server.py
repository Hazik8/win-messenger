#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
import json
import uuid
import uvicorn
import os
from typing import Dict
from cryptography.fernet import Fernet
import base64

app = FastAPI(title="Win Messenger Server")

# CORS для Render
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====================== ШИФРОВАНИЕ ======================
KEY_FILE = "encryption_key.key"

def load_or_generate_key():
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            return f.read()
    else:
        key = Fernet.generate_key()
        with open(KEY_FILE, "wb") as f:
            f.write(key)
        print("🔑 Новый ключ шифрования создан")
        return key

ENCRYPTION_KEY = load_or_generate_key()
cipher = Fernet(ENCRYPTION_KEY)

def encrypt_message(content: str) -> str:
    return base64.urlsafe_b64encode(cipher.encrypt(content.encode())).decode()

def decrypt_message(encrypted_data: str) -> str:
    try:
        return cipher.decrypt(base64.urlsafe_b64decode(encrypted_data)).decode()
    except:
        return "[Ошибка расшифровки]"

# ====================== БАЗА ДАННЫХ ======================
Base = declarative_base()
engine = create_engine('sqlite:///win_messenger.db', echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    user_id = Column(String, unique=True, index=True, nullable=False)
    username = Column(String, nullable=False)
    bio = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

class Group(Base):
    __tablename__ = 'groups'
    id = Column(Integer, primary_key=True)
    group_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Message(Base):
    __tablename__ = 'messages'
    id = Column(Integer, primary_key=True)
    sender_id = Column(Integer, ForeignKey('users.id'))
    receiver_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    group_id = Column(String, nullable=True)
    encrypted_content = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# ====================== WebSocket ======================
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections[user_id] = websocket

    def disconnect(self, user_id: str):
        self.active_connections.pop(user_id, None)

    async def broadcast(self, message: dict):
        for ws in list(self.active_connections.values()):
            try:
                await ws.send_text(json.dumps(message))
            except:
                pass

manager = ConnectionManager()

# ====================== РОУТЫ ======================
@app.post("/register")
async def register(username: str):
    db = SessionLocal()
    try:
        user_id = str(uuid.uuid4())[:8].upper()
        user = User(user_id=user_id, username=username)
        db.add(user)
        db.commit()
        return {"status": "success", "user_id": user_id, "username": username}
    finally:
        db.close()

@app.post("/login")
async def login(user_id: str):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.user_id == user_id).first()
        if user:
            return {"status": "success", "user": {"id": user.user_id, "username": user.username, "bio": user.bio}}
        return {"status": "error", "message": "Пользователь не найден"}
    finally:
        db.close()

@app.put("/profile")
async def update_profile(user_id: str, username: str = None, bio: str = None):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.user_id == user_id).first()
        if user:
            if username: user.username = username
            if bio is not None: user.bio = bio
            db.commit()
            return {"status": "success"}
        return {"status": "error", "message": "Пользователь не найден"}
    finally:
        db.close()

@app.post("/create_group")
async def create_group(name: str):
    db = SessionLocal()
    try:
        group_id = "GRP" + str(uuid.uuid4())[:6].upper()
        group = Group(group_id=group_id, name=name)
        db.add(group)
        db.commit()
        return {"status": "success", "group_id": group_id}
    finally:
        db.close()

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await manager.connect(websocket, user_id)
    db = SessionLocal()
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            if message["action"] == "send_message":
                content = message["content"]
                receiver_id = message.get("receiver_id")
                group_id = message.get("group_id")

                user = db.query(User).filter(User.user_id == user_id).first()
                if not user: continue

                encrypted = encrypt_message(content)

                new_msg = Message(
                    sender_id=user.id,
                    receiver_id=db.query(User).filter_by(user_id=receiver_id).first().id if receiver_id else None,
                    group_id=group_id,
                    encrypted_content=encrypted
                )
                db.add(new_msg)
                db.commit()

                msg_data = {
                    "action": "new_message",
                    "sender": user.username,
                    "content": content,
                    "timestamp": new_msg.timestamp.strftime("%H:%M"),
                    "receiver_id": receiver_id,
                    "group_id": group_id
                }
                await manager.broadcast(msg_data)

            elif message["action"] == "get_messages":
                msgs = db.query(Message).order_by(Message.timestamp.desc()).limit(150).all()
                msg_list = []
                for m in msgs:
                    sender = db.query(User).get(m.sender_id)
                    msg_list.append({
                        "sender": sender.username if sender else "Unknown",
                        "content": decrypt_message(m.encrypted_content),
                        "timestamp": m.timestamp.strftime("%H:%M")
                    })
                await websocket.send_text(json.dumps({"action": "messages", "messages": msg_list}))

    except WebSocketDisconnect:
        manager.disconnect(user_id)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

# ====================== ЗАПУСК ======================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"🚀 Server running on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
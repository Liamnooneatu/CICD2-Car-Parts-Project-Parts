from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
import httpx
import os

app = FastAPI(title="Service B - Users API")

SERVICE_A_BASE_URL = os.getenv("SERVICE_A_BASE_URL", "http://localhost:8001")

class User(BaseModel):
    user_id: int = Field(..., ge=1)
    name: str = Field(..., min_length=1, max_length=100)
    email: EmailStr


users: list[User] = []


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/users", response_model=list[User])
def list_users():
    return users


@app.get("/api/users/{user_id}", response_model=User)
def get_user(user_id: int):
    for u in users:
        if u.user_id == user_id:
            return u
    raise HTTPException(status_code=404, detail="User not found")


@app.post("/api/users", response_model=User, status_code=status.HTTP_201_CREATED)
def create_user(user: User):
    if any(u.user_id == user.user_id for u in users):
        raise HTTPException(status_code=409, detail="user_id already exists")
    users.append(user)
    return user


@app.put("/api/users/{user_id}", response_model=User)
def update_user(user_id: int, updated: User):
    if updated.user_id != user_id:
        raise HTTPException(status_code=400, detail="user_id in body must match URL")

    for i, u in enumerate(users):
        if u.user_id == user_id:
            users[i] = updated
            return updated

    raise HTTPException(status_code=404, detail="User not found")


@app.delete("/api/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(user_id: int):
    for i, u in enumerate(users):
        if u.user_id == user_id:
            users.pop(i)
            return
    raise HTTPException(status_code=404, detail="User not found")


@app.get("/api/users/{user_id}/parts/{part_id}")
def user_views_part(user_id: int, part_id: int):
    """
    Demonstrates a simple sync service-to-service call:
    - Verify user exists in Service B
    - Fetch part details from Service A
    """  
    _ = get_user(user_id)

    url = f"{SERVICE_A_BASE_URL}/api/parts/{part_id}"

    try:
        r = httpx.get(url, timeout=3.0)
    except httpx.RequestError:
        raise HTTPException(status_code=503, detail="Parts service unavailable")

    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Part not found in Parts service")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Unexpected response from Parts service")

    return {
        "user_id": user_id,
        "part_id": part_id,
        "part": r.json(),
    }


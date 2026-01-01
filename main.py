import os
import shutil
import sqlite3
import mimetypes
from datetime import datetime, timezone
from typing import List, Dict, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse

APP_DB = os.environ.get("APP_DB", "app.db")
UPLOAD_ROOT = os.environ.get("UPLOAD_ROOT", "uploads")
MAX_IMAGES_PER_USER = 10

ALLOWED_CONTENT_TYPES = {
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/bmp",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_user_id(user_id: str) -> str:
    # Keep it simple and predictable: disallow path separators and empty ids.
    if not user_id or user_id.strip() == "":
        raise HTTPException(status_code=400, detail="user_id is required")
    if "/" in user_id or "\\" in user_id or ".." in user_id:
        raise HTTPException(status_code=400, detail="invalid user_id")
    return user_id


def ensure_dirs():
    os.makedirs(UPLOAD_ROOT, exist_ok=True)


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(APP_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS images (
                user_id TEXT NOT NULL,
                image_index INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (user_id, image_index)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_images_user_created ON images(user_id, created_at)")
        conn.commit()


def user_dir(user_id: str) -> str:
    return os.path.join(UPLOAD_ROOT, user_id)


def user_exists(user_id: str) -> bool:
    # "User" existence is purely whether their folder exists.
    return os.path.isdir(user_dir(user_id))


def get_user_images(conn: sqlite3.Connection, user_id: str) -> List[sqlite3.Row]:
    cur = conn.execute(
        "SELECT user_id, image_index, file_path, created_at FROM images WHERE user_id=? ORDER BY image_index ASC",
        (user_id,),
    )
    return cur.fetchall()


def pick_index_for_upload(conn: sqlite3.Connection, user_id: str) -> int:
    rows = get_user_images(conn, user_id)

    if len(rows) < MAX_IMAGES_PER_USER:
        used = {int(r["image_index"]) for r in rows}
        for i in range(MAX_IMAGES_PER_USER):
            if i not in used:
                return i
        # Should never happen, but keep boring:
        return 0

    # Replacement policy for #11+: replace oldest (first uploaded), then next, etc.
    # Oldest is smallest created_at. We reuse its index.
    cur = conn.execute(
        """
        SELECT image_index, file_path, created_at
        FROM images
        WHERE user_id=?
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (user_id,),
    )
    oldest = cur.fetchone()
    if oldest is None:
        return 0
    return int(oldest["image_index"])


def delete_existing_at_index(conn: sqlite3.Connection, user_id: str, idx: int) -> None:
    cur = conn.execute(
        "SELECT file_path FROM images WHERE user_id=? AND image_index=?",
        (user_id, idx),
    )
    row = cur.fetchone()
    if row is None:
        return

    path = row["file_path"]
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except OSError:
        # Don't overcomplicate: if deletion fails, we still proceed with DB update.
        pass

    conn.execute("DELETE FROM images WHERE user_id=? AND image_index=?", (user_id, idx))


def infer_extension(upload: UploadFile) -> str:
    # Prefer filename extension; fallback to content-type.
    name = upload.filename or ""
    _, ext = os.path.splitext(name)
    ext = (ext or "").lower()

    if ext in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}:
        return ext

    ct = (upload.content_type or "").lower()
    if ct == "image/png":
        return ".png"
    if ct == "image/jpeg":
        return ".jpg"
    if ct == "image/gif":
        return ".gif"
    if ct == "image/webp":
        return ".webp"
    if ct == "image/bmp":
        return ".bmp"

    # Default (boring): png
    return ".png"


app = FastAPI()


@app.on_event("startup")
def startup():
    ensure_dirs()
    db_init()


@app.post("/users/{user_id}")
def create_user(user_id: str):
    user_id = safe_user_id(user_id)
    os.makedirs(user_dir(user_id), exist_ok=True)
    return {"ok": True}


@app.post("/users/{user_id}/images")
def upload_image(user_id: str, file: UploadFile = File(...)):
    user_id = safe_user_id(user_id)

    # Require explicit user creation (folder must exist)
    if not user_exists(user_id):
        raise HTTPException(status_code=404, detail="User not found. Create user first.")

    content_type = (file.content_type or "").lower()
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported image type")

    ensure_dirs()
    os.makedirs(user_dir(user_id), exist_ok=True)

    ext = infer_extension(file)

    with db_connect() as conn:
        idx = pick_index_for_upload(conn, user_id)

        # If replacing, delete existing row + file first
        delete_existing_at_index(conn, user_id, idx)

        path = os.path.join(user_dir(user_id), f"{idx}{ext}")

        # Write to disk (streaming, not in memory)
        try:
            with open(path, "wb") as out:
                shutil.copyfileobj(file.file, out)
        finally:
            try:
                file.file.close()
            except Exception:
                pass

        created_at = utc_now_iso()
        conn.execute(
            "INSERT INTO images(user_id, image_index, file_path, created_at) VALUES(?,?,?,?)",
            (user_id, idx, path, created_at),
        )
        conn.commit()

    return {"index": idx, "url": f"/users/{user_id}/images/{idx}"}


@app.get("/users/{user_id}/images/{index}")
def get_single_image(user_id: str, index: int):
    user_id = safe_user_id(user_id)
    if index < 0 or index >= MAX_IMAGES_PER_USER:
        raise HTTPException(status_code=404, detail="Image not found")

    with db_connect() as conn:
        cur = conn.execute(
            "SELECT file_path FROM images WHERE user_id=? AND image_index=?",
            (user_id, int(index)),
        )
        row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Image not found")

    path = row["file_path"]
    if not path or not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Image not found")

    media_type, _ = mimetypes.guess_type(path)
    return FileResponse(path, media_type=media_type or "application/octet-stream")


@app.get("/users/{user_id}/images")
def get_all_images(user_id: str) -> List[Dict[str, object]]:
    user_id = safe_user_id(user_id)
    if not user_exists(user_id):
        raise HTTPException(status_code=404, detail="User not found. Create user first.")

    with db_connect() as conn:
        rows = get_user_images(conn, user_id)

    return [
        {"index": int(r["image_index"]), "url": f"/users/{user_id}/images/{int(r['image_index'])}"}
        for r in rows
    ]

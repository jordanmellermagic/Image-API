import os
import shutil
import sqlite3
import mimetypes
from datetime import datetime, timezone

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

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


# -------------------------
# Utilities
# -------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_user_id(user_id: str) -> str:
    if not user_id or user_id.strip() == "":
        raise HTTPException(status_code=400, detail="user_id required")
    if "/" in user_id or "\\" in user_id or ".." in user_id:
        raise HTTPException(status_code=400, detail="invalid user_id")
    return user_id


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
        conn.commit()


def user_dir(user_id: str) -> str:
    return os.path.join(UPLOAD_ROOT, user_id)


def user_exists(user_id: str) -> bool:
    return os.path.isdir(user_dir(user_id))


def infer_extension(upload: UploadFile) -> str:
    name = upload.filename or ""
    _, ext = os.path.splitext(name.lower())

    if ext in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}:
        return ext

    return {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/bmp": ".bmp",
    }.get(upload.content_type, ".png")


# -------------------------
# App
# -------------------------

app = FastAPI()

# ✅ REQUIRED FOR WEB APP (Render Static Site)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://imager-viewer.onrender.com",
        "http://localhost:3000",
        "http://localhost:8000",
    ],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    os.makedirs(UPLOAD_ROOT, exist_ok=True)
    db_init()


# -------------------------
# Endpoints
# -------------------------

@app.post("/users/{user_id}")
def create_user(user_id: str):
    user_id = safe_user_id(user_id)
    os.makedirs(user_dir(user_id), exist_ok=True)
    return {"ok": True}


@app.post("/users/{user_id}/images")
def upload_image(user_id: str, file: UploadFile = File(...)):
    user_id = safe_user_id(user_id)

    if not user_exists(user_id):
        raise HTTPException(status_code=404, detail="User not found")

    if file.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported image type")

    os.makedirs(user_dir(user_id), exist_ok=True)

    with db_connect() as conn:
        rows = conn.execute(
            "SELECT image_index, created_at FROM images WHERE user_id=? ORDER BY created_at ASC",
            (user_id,),
        ).fetchall()

        if len(rows) < MAX_IMAGES_PER_USER:
            used = {r["image_index"] for r in rows}
            index = next(i for i in range(MAX_IMAGES_PER_USER) if i not in used)
        else:
            index = rows[0]["image_index"]
            old = conn.execute(
                "SELECT file_path FROM images WHERE user_id=? AND image_index=?",
                (user_id, index),
            ).fetchone()
            if old and os.path.isfile(old["file_path"]):
                os.remove(old["file_path"])
            conn.execute(
                "DELETE FROM images WHERE user_id=? AND image_index=?",
                (user_id, index),
            )

        ext = infer_extension(file)
        path = os.path.join(user_dir(user_id), f"{index}{ext}")

        with open(path, "wb") as out:
            shutil.copyfileobj(file.file, out)

        conn.execute(
            "INSERT INTO images (user_id, image_index, file_path, created_at) VALUES (?,?,?,?)",
            (user_id, index, path, utc_now_iso()),
        )
        conn.commit()

    return {"index": index, "url": f"/users/{user_id}/images/{index}"}


@app.get("/users/{user_id}/images/{index}")
def get_single_image(user_id: str, index: int):
    user_id = safe_user_id(user_id)

    with db_connect() as conn:
        row = conn.execute(
            "SELECT file_path FROM images WHERE user_id=? AND image_index=?",
            (user_id, index),
        ).fetchone()

    if not row or not os.path.isfile(row["file_path"]):
        raise HTTPException(status_code=404, detail="Image not found")

    media_type, _ = mimetypes.guess_type(row["file_path"])
    return FileResponse(row["file_path"], media_type=media_type or "application/octet-stream")


@app.get("/users/{user_id}/images", response_class=HTMLResponse)
def get_all_images(user_id: str):
    user_id = safe_user_id(user_id)

    with db_connect() as conn:
        rows = conn.execute(
            "SELECT image_index FROM images WHERE user_id=? ORDER BY image_index ASC",
            (user_id,),
        ).fetchall()

    tiles = ""
    for r in rows:
        i = r["image_index"]
        tiles += f"""
        <div style="width:200px">
            <img src="/users/{user_id}/images/{i}" style="width:100%; border:1px solid #ccc"/>
            <div style="text-align:center">#{i}</div>
        </div>
        """

    return f"""
    <!doctype html>
    <html>
    <head>
        <title>{user_id} images</title>
    </head>
    <body style="font-family:sans-serif">
        <h2>User: {user_id}</h2>
        <div style="
            display:grid;
            grid-template-columns:repeat(auto-fill,minmax(200px,1fr));
            gap:16px;">
            {tiles}
        </div>
    </body>
    </html>
    """


@app.delete("/users/{user_id}/images")
def clear_all_images(user_id: str):
    user_id = safe_user_id(user_id)
    folder = user_dir(user_id)

    with db_connect() as conn:
        conn.execute("DELETE FROM images WHERE user_id=?", (user_id,))
        conn.commit()

    if os.path.isdir(folder):
        for f in os.listdir(folder):
            path = os.path.join(folder, f)
            if os.path.isfile(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

    return {"ok": True}

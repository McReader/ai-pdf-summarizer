import logging
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, File, HTTPException, Query, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware

from redis_client import get_redis_client

app = FastAPI(
    title="PDF Summary API",
    description="Backend API for PDF summarization application",
    version="1.0.0"
)

MAX_PDF_SIZE_BYTES = 5 * 1024 * 1024  # 5MB
STREAM_INGESTED = "pdf:ingested"
BIN_KEY_TEMPLATE = "pdf:bin:{file_id}"
META_KEY_TEMPLATE = "pdf:meta:{file_id}"

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://frontend:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


def _decode_hash(raw: dict | None) -> dict:
    if not raw:
        return {}
    decoded = {}
    for key, value in raw.items():
        k = key.decode() if isinstance(key, (bytes, bytearray)) else key
        v = value.decode() if isinstance(value, (bytes, bytearray)) else value
        decoded[k] = v
    return decoded


@app.get("/summaries")
async def get_all_summaries():
    """Get all summaries from Redis"""
    redis_client = get_redis_client()
    pattern = META_KEY_TEMPLATE.format(file_id="*")
    
    summaries = []
    for key in redis_client.scan_iter(match=pattern):
        meta = redis_client.hgetall(key)
        if meta:
            decoded = _decode_hash(meta)
            summaries.append({
                "file_id": decoded.get("file_id", ""),
                "filename": decoded.get("filename", ""),
                "status": decoded.get("status", "unknown"),
                "text": decoded.get("text", ""),
                "summary": decoded.get("summary", ""),
                "updated_at": decoded.get("updated_at"),
            })
    
    return {"summaries": summaries, "count": len(summaries)}


@app.get("/status/{file_id}")
async def get_file_status(file_id: str):
    redis_client = get_redis_client()
    meta_key = META_KEY_TEMPLATE.format(file_id=file_id)
    meta = redis_client.hgetall(meta_key)

    if not meta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found.",
        )

    decoded = _decode_hash(meta)
    return {
        "file_id": decoded.get("file_id", file_id),
        "filename": decoded.get("filename", ""),
        "status": decoded.get("status", "unknown"),
        "text": decoded.get("text", ""),
        "summary": decoded.get("summary", ""),
        "updated_at": decoded.get("updated_at"),
    }

@app.post("/summarize")
async def summarize_pdf(
    file: UploadFile = File(...),
    mode: str = Query(default="plain_text", description="Text extraction mode: 'plain_text' or 'markdown'")
):
    if file.content_type not in ("application/pdf", "application/x-pdf"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only PDF uploads are supported.",
        )

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Empty file uploads are not allowed.",
        )

    if len(file_bytes) > MAX_PDF_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File too large. Maximum allowed size is 5MB.",
        )

    file_id = str(uuid.uuid4())
    bin_key = BIN_KEY_TEMPLATE.format(file_id=file_id)
    meta_key = META_KEY_TEMPLATE.format(file_id=file_id)

    redis_client = get_redis_client()

    try:
        redis_client.set(bin_key, file_bytes)
        redis_client.hset(
            meta_key,
            mapping={
                "file_id": file_id,
                "filename": file.filename or "",
                "status": "uploaded",
                "text": "",
                "summary": "",
                "extraction_mode": mode,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        redis_client.xadd(
            STREAM_INGESTED,
            {
                "file_id": file_id,
                "bin_key": bin_key,
                "meta_key": meta_key,
                "filename": file.filename or "",
                "extraction_mode": mode,
            },
        )
    except Exception as exc:  # pragma: no cover - defensive for Redis issues
        logging.error(f"Failed to enqueue PDF for processing: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to enqueue PDF for processing.",
        ) from exc

    return {
        "file_id": file_id,
        "status": "uploaded",
    }


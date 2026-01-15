import io
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict
from google import genai
from google.genai import types

from pypdf import PdfReader

from redis_client import get_redis_client

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s      %(message)s',
    stream=sys.stdout
)

STREAM_INGESTED = "pdf:ingested"
STREAM_TEXT_READY = "pdf:text_ready"
BIN_KEY_TEMPLATE = "pdf:bin:{file_id}"
META_KEY_TEMPLATE = "pdf:meta:{file_id}"
CONSUMER_GROUP = "text_extraction_handlers"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decode_field(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode()
    return str(value)


def _decode_stream_fields(fields: Dict[Any, Any]) -> Dict[str, str]:
    return {_decode_field(k): _decode_field(v) for k, v in fields.items()}


def _update_meta(meta_key: str, mapping: Dict[str, Any]) -> None:
    client = get_redis_client()
    mapping["updated_at"] = _now_iso()
    client.hset(meta_key, mapping=mapping)


def _get_consumer_name() -> str:
    """Generate a unique consumer name for this instance."""
    pid = os.getpid()
    return f"text-extraction-handler-{pid}"


def _ensure_consumer_group(client) -> None:
    """
    Verify the consumer group exists (fallback - should be created by backend on startup).
    This is a lightweight check that only creates if missing.
    """
    try:
        client.xgroup_create(
            name=STREAM_INGESTED,
            groupname=CONSUMER_GROUP,
            id="0",
            mkstream=True  # Create stream if it doesn't exist
        )
        logging.info("Created consumer group '%s' for stream '%s'", CONSUMER_GROUP, STREAM_INGESTED)
    except Exception as exc:
        error_msg = str(exc).lower()
        if "busygroup" in error_msg or "already exists" in error_msg:
            logging.info("Consumer group '%s' already exists", CONSUMER_GROUP)
        else:
            logging.warning("Unexpected error creating consumer group: %s", exc)


def extract_markdown_from_pdf(pdf_bytes: bytes) -> str:
    client = genai.Client()

    prompt = "Extract the text from the PDF in markdown format"
    response = client.models.generate_content(
        model="gemini-2.5-flash-lite",
        contents=[
            types.Part.from_bytes(
                data=pdf_bytes,
                mime_type='application/pdf',
            ),
            prompt,
        ],
    )

    return response.text


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def _process_message(fields: Dict[Any, Any], client) -> bool:
    """
    Process a single message from the stream.
    Returns True if processing was successful and message should be acknowledged.
    """
    fields = _decode_stream_fields(fields)
    file_id = fields.get("file_id")
    bin_key = fields.get("bin_key") or BIN_KEY_TEMPLATE.format(file_id=file_id)
    meta_key = fields.get("meta_key") or META_KEY_TEMPLATE.format(file_id=file_id)
    extraction_mode = fields.get("extraction_mode")

    logging.info("Extracting text from the file: %s in mode: %s", file_id, extraction_mode)

    if not file_id:
        logging.warning("Received ingested event without file_id")
        return True

    pdf_bytes = client.get(bin_key)
    if not pdf_bytes:
        logging.error("PDF binary not found for file_id=%s", file_id)
        _update_meta(
            meta_key,
            {"status": "error", "error": "binary_missing"},
        )
        return True

    try:
        if extraction_mode == "markdown":
            extracted_text = extract_markdown_from_pdf(pdf_bytes)
        else:
            extracted_text = extract_text_from_pdf(pdf_bytes)
    except Exception as exc:  # pragma: no cover - defensive
        logging.exception("Failed to extract text for %s: %s", file_id, exc)
        _update_meta(
            meta_key,
            {"status": "error", "error": "text_extraction_failed"},
        )
        return False

    _update_meta(
        meta_key,
        {"status": "text_ready", "text": extracted_text},
    )

    client.xadd(
        STREAM_TEXT_READY,
        {
            "file_id": file_id,
            "meta_key": meta_key,
            "extraction_mode": extraction_mode,
        },
    )
    
    return True


def consume_ingested_stream() -> None:
    client = get_redis_client()
    consumer_name = _get_consumer_name()
    
    _ensure_consumer_group(client)
    
    logging.info("Starting consumer '%s' in group '%s'", consumer_name, CONSUMER_GROUP)
    
    while True:
        try:
            entries = client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=consumer_name,
                streams={STREAM_INGESTED: ">"},
                count=100,
                block=5000
            )
            
            if not entries:
                continue

            for _, messages in entries:
                for message_id, raw_fields in messages:
                    success = _process_message(raw_fields, client)
                    if success:
                        logging.info("Acknowledged message: %s", message_id)
                        client.xack(STREAM_INGESTED, CONSUMER_GROUP, message_id)
                    
        except Exception as exc:  # pragma: no cover - defensive
            logging.exception("Error consuming ingested stream: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    logging.info("Starting text extraction worker")
    consume_ingested_stream()


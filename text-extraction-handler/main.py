import io
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict
from google import genai
from google.genai import types

from pypdf import PdfReader

from redis_client import get_redis_client

# Configure logging to output to stdout/stderr for Docker
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s      %(message)s',
    stream=sys.stdout
)

STREAM_INGESTED = "pdf:ingested"
STREAM_TEXT_READY = "pdf:text_ready"
BIN_KEY_TEMPLATE = "pdf:bin:{file_id}"
META_KEY_TEMPLATE = "pdf:meta:{file_id}"

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


def consume_ingested_stream() -> None:
    client = get_redis_client()
    last_id = "$"
    while True:
        try:
            entries = client.xread({STREAM_INGESTED: last_id}, block=5000, count=1)
            if not entries:
                continue

            for _, messages in entries:
                for message_id, raw_fields in messages:
                    last_id = message_id
                    fields = _decode_stream_fields(raw_fields)
                    file_id = fields.get("file_id")
                    bin_key = fields.get("bin_key") or BIN_KEY_TEMPLATE.format(file_id=file_id)
                    meta_key = fields.get("meta_key") or META_KEY_TEMPLATE.format(file_id=file_id)
                    extraction_mode = fields.get("extraction_mode")

                    logging.info("Extracting text from the file: %s in mode: %s", file_id, extraction_mode)

                    if not file_id:
                        logging.warning("Received ingested event without file_id")
                        continue

                    pdf_bytes = client.get(bin_key)
                    if not pdf_bytes:
                        logging.error("PDF binary not found for file_id=%s", file_id)
                        _update_meta(
                            meta_key,
                            {"status": "error", "error": "binary_missing"},
                        )
                        continue

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
                        continue

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
        except Exception as exc:  # pragma: no cover - defensive
            logging.exception("Error consuming ingested stream: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    logging.info("Starting text extraction worker")
    consume_ingested_stream()


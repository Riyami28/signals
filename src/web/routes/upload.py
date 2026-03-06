"""CSV upload endpoint with AI column detection and validation (Issue #37)."""

from __future__ import annotations

import csv
import io
import json
import logging
import uuid
from typing import Optional

import anthropic
import httpx
from fastapi import APIRouter, HTTPException, UploadFile

from src import db
from src.settings import load_settings
from src.utils import normalize_domain

logger = logging.getLogger(__name__)

router = APIRouter(tags=["upload"])

MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB
MAX_ROWS = 10_000
MIN_ROWS = 1

# Known header variations that map to our canonical columns.
_HEADER_ALIASES: dict[str, list[str]] = {
    "company_name": [
        "company",
        "company name",
        "company_name",
        "name",
        "organization",
        "org",
        "org_name",
        "business",
        "business_name",
        "account",
        "account_name",
        "client",
        "client_name",
        "firm",
    ],
    "domain": [
        "domain",
        "website",
        "url",
        "web",
        "site",
        "homepage",
        "company_url",
        "company_website",
        "company_domain",
        "web_url",
    ],
    "industry": [
        "industry",
        "sector",
        "vertical",
        "segment",
        "category",
        "business_type",
        "industry_type",
    ],
    "employee_count": [
        "employee_count",
        "employees",
        "employee count",
        "headcount",
        "head_count",
        "size",
        "company_size",
        "num_employees",
        "number_of_employees",
        "team_size",
    ],
    "location": [
        "location",
        "city",
        "country",
        "region",
        "headquarters",
        "hq",
        "address",
        "state",
        "geo",
        "geography",
    ],
}


def _get_conn():
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    return conn


# ---------------------------------------------------------------------------
# Header-based column mapping (fast, no API call)
# ---------------------------------------------------------------------------


def _match_headers_by_alias(headers: list[str]) -> dict[str, str]:
    """Try to map CSV headers to canonical columns using known aliases.

    Returns a dict of {canonical_column: csv_header} for matched columns.
    """
    mapping: dict[str, str] = {}
    for header in headers:
        normalized = header.strip().lower().replace("-", "_").replace(" ", "_")
        for canonical, aliases in _HEADER_ALIASES.items():
            if normalized in [a.replace(" ", "_").replace("-", "_") for a in aliases]:
                if canonical not in mapping:
                    mapping[canonical] = header
                break
    return mapping


# ---------------------------------------------------------------------------
# AI-powered column detection (fallback when alias matching is insufficient)
# ---------------------------------------------------------------------------

_AI_PARSE_SYSTEM = (
    "You are a data parsing assistant. Given CSV column headers and a few sample rows, "
    "identify which columns map to: company_name, domain, industry, employee_count, location. "
    "Return ONLY a JSON object mapping our canonical names to the original CSV header names. "
    "Only include mappings you are confident about. Example response:\n"
    '{"company_name": "Organization", "domain": "Website URL", "industry": "Sector"}\n'
    "Do not include any explanation, markdown, or extra text. Return only the JSON object."
)


def _ai_detect_columns(
    headers: list[str],
    sample_rows: list[dict[str, str]],
    api_key: str,
    model: str = "claude-sonnet-4-5",
    timeout: int = 30,
    provider: str = "claude",
) -> dict[str, str]:
    """Use LLM to detect column mappings from headers + sample data."""
    sample_text = "Headers: " + ", ".join(headers) + "\n\nSample rows:\n"
    for row in sample_rows[:5]:
        sample_text += json.dumps(row, ensure_ascii=False) + "\n"

    if provider == "minimax":
        with httpx.Client(
            base_url="https://api.minimax.io/v1",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        ) as http_client:
            response = http_client.post(
                "/chat/completions",
                json={
                    "model": model,
                    "max_tokens": 256,
                    "messages": [
                        {"role": "system", "content": _AI_PARSE_SYSTEM},
                        {"role": "user", "content": sample_text},
                    ],
                },
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
        choices = data.get("choices") or []
        first = choices[0] if choices else {}
        msg = first.get("message") if isinstance(first, dict) else {}
        raw = (msg or {}).get("content") or "{}"
    else:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=model,
            max_tokens=256,
            system=_AI_PARSE_SYSTEM,
            messages=[{"role": "user", "content": sample_text}],
            timeout=timeout,
        )
        raw = message.content[0].text if message.content else "{}"
    # Strip any markdown fencing the model might add.
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("ai_detect_columns: failed to parse response: %s", raw)
        return {}

    # Validate that returned headers actually exist in the CSV.
    valid: dict[str, str] = {}
    canonical_names = set(_HEADER_ALIASES.keys())
    for key, val in result.items():
        if key in canonical_names and val in headers:
            valid[key] = val
    return valid


# ---------------------------------------------------------------------------
# CSV parsing + validation
# ---------------------------------------------------------------------------


def _parse_employee_count(value: str) -> Optional[int]:
    """Best-effort parse of employee count strings like '1,500' or '1500+'."""
    if not value:
        return None
    cleaned = value.strip().replace(",", "").replace("+", "").replace("~", "")
    # Handle range like "100-500" — take the first number.
    if "-" in cleaned:
        cleaned = cleaned.split("-")[0].strip()
    try:
        return int(cleaned)
    except (ValueError, TypeError):
        return None


def _parse_and_validate_csv(
    content: str,
    ai_api_key: str,
    ai_model: str,
    ai_provider: str = "claude",
) -> tuple[list[dict], dict[str, str], list[str]]:
    """Parse CSV content, detect columns, validate rows.

    Returns:
        (parsed_rows, column_mapping, validation_errors)
    """
    validation_errors: list[str] = []

    # Parse CSV.
    try:
        reader = csv.DictReader(io.StringIO(content))
        headers = reader.fieldnames or []
    except csv.Error as exc:
        return [], {}, [f"CSV parse error: {exc}"]

    if not headers:
        return [], {}, ["CSV file has no headers"]

    # Read all rows.
    raw_rows: list[dict[str, str]] = []
    for row in reader:
        raw_rows.append(row)
        if len(raw_rows) > MAX_ROWS:
            return [], {}, [f"CSV exceeds maximum of {MAX_ROWS:,} rows"]

    if len(raw_rows) < MIN_ROWS:
        return [], {}, ["CSV file has no data rows"]

    # Step 1: Try alias-based header matching.
    mapping = _match_headers_by_alias(list(headers))

    # Step 2: If we don't have at least company_name or domain, try AI.
    if not (mapping.get("company_name") or mapping.get("domain")):
        if ai_api_key:
            logger.info("alias matching insufficient, using AI column detection")
            ai_mapping = _ai_detect_columns(
                list(headers),
                raw_rows[:5],
                ai_api_key,
                ai_model,
                provider=ai_provider,
            )
            # Merge AI results (don't overwrite alias matches).
            for key, val in ai_mapping.items():
                if key not in mapping:
                    mapping[key] = val
        else:
            logger.warning("no AI API key available, skipping AI column detection")

    # Validate that we have at least company_name or domain.
    if not (mapping.get("company_name") or mapping.get("domain")):
        return [], mapping, [f"Could not detect a company name or domain column. Headers found: {', '.join(headers)}"]

    # Step 3: Transform rows using the mapping.
    parsed_rows: list[dict] = []
    seen_domains: set[str] = set()
    missing_domain_count = 0

    for i, row in enumerate(raw_rows, start=2):  # Row 2 = first data row (1-indexed + header)
        company_name = ""
        domain = ""
        industry = ""
        employee_count = None
        location = ""
        extra_metadata: dict[str, str] = {}

        # Extract mapped columns.
        if "company_name" in mapping:
            company_name = (row.get(mapping["company_name"]) or "").strip()
        if "domain" in mapping:
            domain = normalize_domain(row.get(mapping["domain"]) or "")
        if "industry" in mapping:
            industry = (row.get(mapping["industry"]) or "").strip()
        if "employee_count" in mapping:
            employee_count = _parse_employee_count(row.get(mapping["employee_count"]) or "")
        if "location" in mapping:
            location = (row.get(mapping["location"]) or "").strip()

        # Collect unmapped columns as metadata.
        mapped_headers = set(mapping.values())
        for header in headers:
            if header not in mapped_headers:
                val = (row.get(header) or "").strip()
                if val:
                    extra_metadata[header] = val

        # Add location to metadata if present.
        if location:
            extra_metadata["location"] = location

        # Skip rows with neither company name nor domain.
        if not company_name and not domain:
            validation_errors.append(f"Row {i}: missing both company name and domain, skipped")
            continue

        # Flag rows missing domain.
        if not domain:
            missing_domain_count += 1
            validation_errors.append(f"Row {i}: missing domain for '{company_name}', flagged for review")

        # Within-batch dedup by domain.
        if domain:
            if domain in seen_domains:
                validation_errors.append(f"Row {i}: duplicate domain '{domain}', skipped")
                continue
            seen_domains.add(domain)

        parsed_rows.append(
            {
                "company_name": company_name,
                "domain": domain,
                "industry": industry,
                "employee_count": employee_count,
                "metadata": extra_metadata,
            }
        )

    if missing_domain_count > 0:
        validation_errors.append(f"{missing_domain_count} row(s) missing domain — flagged for manual review")

    return parsed_rows, mapping, validation_errors


# ---------------------------------------------------------------------------
# API endpoint
# ---------------------------------------------------------------------------


@router.post("/v1/upload/csv")
async def upload_csv(file: UploadFile):
    """Upload a CSV file of companies for batch processing.

    Accepts multipart/form-data with a CSV file. Uses AI to detect
    and normalize columns, validates data, deduplicates by domain,
    and stores as a batch for pipeline processing.
    """
    # Validate file type.
    filename = file.filename or "upload.csv"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    # Read file content with size check.
    raw_bytes = await file.read()
    if len(raw_bytes) > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"File exceeds maximum size of {MAX_FILE_SIZE_BYTES // (1024 * 1024)} MB",
        )
    if len(raw_bytes) == 0:
        raise HTTPException(status_code=400, detail="File is empty")

    # Decode CSV content.
    try:
        content = raw_bytes.decode("utf-8-sig")  # Handle BOM.
    except UnicodeDecodeError:
        try:
            content = raw_bytes.decode("latin-1")
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="Unable to decode file encoding")

    # Load settings for AI API key.
    settings = load_settings()
    provider = getattr(settings, "llm_provider", "claude")
    if provider == "minimax":
        ai_api_key = settings.minimax_api_key
        ai_model = settings.minimax_model
    else:
        ai_api_key = settings.claude_api_key
        ai_model = settings.claude_model

    # Parse, detect columns, and validate.
    parsed_rows, column_mapping, validation_errors = _parse_and_validate_csv(
        content,
        ai_api_key,
        ai_model,
        ai_provider=provider,
    )

    if not parsed_rows and validation_errors:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "CSV validation failed",
                "validation_errors": validation_errors,
                "parsed_columns": column_mapping,
            },
        )

    # Generate batch ID and store.
    batch_id = f"batch_{uuid.uuid4().hex[:12]}"

    conn = _get_conn()
    try:
        metadata = {
            "original_filename": filename,
            "parsed_columns": column_mapping,
            "validation_errors": validation_errors,
        }
        db.create_upload_batch(conn, batch_id, filename, len(parsed_rows), metadata)

        for row in parsed_rows:
            db.insert_batch_company(
                conn,
                batch_id,
                company_name=row["company_name"],
                domain=row["domain"],
                industry=row.get("industry", ""),
                employee_count=row.get("employee_count"),
                metadata=row.get("metadata", {}),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("failed to store batch %s", batch_id)
        raise HTTPException(status_code=500, detail="Failed to store batch")
    finally:
        conn.close()

    return {
        "batch_id": batch_id,
        "row_count": len(parsed_rows),
        "parsed_columns": column_mapping,
        "validation_errors": validation_errors,
    }

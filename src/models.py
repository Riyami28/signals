from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from src.utils import utc_now_iso

Product = Literal["zopdev", "zopday", "zopnight", "shared"]
Decision = Literal["approved", "rejected", "needs_more_info"]


class Account(BaseModel):
    account_id: str
    company_name: str
    domain: str
    source_type: Literal["seed", "discovered"] = "seed"
    created_at: str = Field(default_factory=utc_now_iso)


class SignalObservation(BaseModel):
    obs_id: str
    account_id: str
    signal_code: str
    product: Product = "shared"
    source: str
    observed_at: str
    evidence_url: str = ""
    evidence_text: str = ""
    document_id: str = ""
    mention_id: str = ""
    evidence_sentence: str = ""
    evidence_sentence_en: str = ""
    matched_phrase: str = ""
    language: str = ""
    speaker_name: str = ""
    speaker_role: str = ""
    evidence_quality: float = 0.0
    relevance_score: float = 0.0
    confidence: float
    source_reliability: float
    raw_payload_hash: str


class ComponentScore(BaseModel):
    run_id: str
    account_id: str
    product: Literal["zopdev", "zopday", "zopnight"]
    signal_code: str
    component_score: float


class AccountScore(BaseModel):
    run_id: str
    account_id: str
    product: Literal["zopdev", "zopday", "zopnight"]
    score: float
    tier: Literal["high", "medium", "low"]
    tier_v2: Literal["tier_1", "tier_2", "tier_3", "tier_4"] = "tier_4"
    top_reasons_json: str
    delta_7d: float
    velocity_7d: float = 0.0
    velocity_14d: float = 0.0
    velocity_30d: float = 0.0
    velocity_category: Literal["surging", "accelerating", "stable", "decelerating"] = "stable"
    confidence_band: Literal["high", "medium", "low"] = "low"
    dimension_scores_json: str = "{}"
    dimension_confidence_json: str = "{}"


class ReviewLabel(BaseModel):
    review_id: str
    run_id: str
    account_id: str
    decision: Decision
    reviewer: str
    notes: str = ""
    created_at: str = Field(default_factory=utc_now_iso)


class Reason(BaseModel):
    signal_code: str
    component_score: float
    source: str
    evidence_url: str = ""
    evidence_text: str = ""


class RunResult(BaseModel):
    run_id: str
    run_date: str
    status: Literal["running", "completed", "failed"]
    details: dict[str, Any] = Field(default_factory=dict)


class EnrichmentData(BaseModel):
    website: str = ""
    industry: str = ""
    sub_industry: str = ""
    employees: int | None = None
    employee_range: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    company_linkedin_url: str = ""
    revenue_range: str = ""
    tech_stack: list[str] = Field(default_factory=list)
    funding_stage: str = ""
    total_funding: float | None = None

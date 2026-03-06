"""LLM-based tweet classification for semantic signal detection.

Replaces keyword matching with batched LLM calls that classify tweets into
signal codes from signal_registry.csv.  Designed for cost-controlled operation:
batch 15 tweets per LLM call, with keyword-matching fallback on failure.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Valid signal codes the LLM may produce (must exist in signal_registry.csv)
# ---------------------------------------------------------------------------
VALID_SIGNAL_CODES: frozenset[str] = frozenset(
    {
        # Hiring
        "devops_role_open",
        "finops_role_open",
        "platform_role_open",
        "general_hiring_activity",
        "hiring_devops",
        # Trigger events
        "launch_or_scale_event",
        "compliance_initiative",
        "audit_date_announced",
        "cloud_migration_intent",
        "enterprise_modernization_program",
        "media_traffic_reliability_pressure",
        "security_review_started",
        # Spend / cost
        "cost_reduction_mandate",
        "cloud_cost_spike",
        "cost_optimization",
        "high_intent_phrase_cost_control",
        # Tech / tooling
        "kubernetes_detected",
        "terraform_detected",
        "gitops_detected",
        "tooling_sprawl_detected",
        "multi_cloud_strategy",
        "cloud_platform_messaging",
        "data_platform_initiative",
        # Behavioral
        "high_intent_phrase_devops_toil",
        "high_intent_phrase_production_fast",
        "devops_bottleneck_language",
        "env_spinup_requests",
        "idp_golden_path_initiative",
        "security_baseline_as_default",
        "vendor_consolidation_program",
        "compliance_governance_messaging",
        # Tooling eval
        "finops_tool_eval",
        # Firmographic
        "recent_funding_event",
        "funding_stage_series_a",
        "funding_stage_series_b_plus",
        "employee_growth_positive",
        "company_news_mention",
    }
)

# Signal code descriptions for the LLM prompt
_SIGNAL_DESCRIPTIONS: dict[str, str] = {
    "devops_role_open": "Hiring for DevOps, SRE, or infrastructure engineer roles",
    "finops_role_open": "Hiring for FinOps or cloud cost management roles",
    "platform_role_open": "Hiring for platform engineering or IDP roles",
    "general_hiring_activity": "General significant hiring activity",
    "hiring_devops": "Specific DevOps/SRE engineer opening mentioned",
    "launch_or_scale_event": "Product launch, new features, or scaling milestone",
    "compliance_initiative": "SOC2, ISO 27001, HIPAA compliance activity",
    "audit_date_announced": "Security or compliance audit announced/completed",
    "cloud_migration_intent": "Plans to migrate to cloud or modernize infrastructure",
    "enterprise_modernization_program": "Digital transformation or IT modernization",
    "media_traffic_reliability_pressure": "Traffic spikes, outages, or reliability concerns",
    "security_review_started": "Penetration testing or security assessment underway",
    "cost_reduction_mandate": "Explicit cost cutting or cloud spend reduction program",
    "cloud_cost_spike": "Complaints about cloud bills or unexpected cost increases",
    "cost_optimization": "General cost optimization or efficiency initiatives",
    "high_intent_phrase_cost_control": "Strong language about infrastructure/cloud costs being a problem",
    "kubernetes_detected": "Using, evaluating, or discussing Kubernetes/containers",
    "terraform_detected": "Using or discussing Terraform/IaC",
    "gitops_detected": "Adopting or discussing GitOps practices",
    "tooling_sprawl_detected": "Complaints about too many tools or tool fragmentation",
    "multi_cloud_strategy": "Multi-cloud or hybrid cloud strategy mentions",
    "cloud_platform_messaging": "Cloud-native or cloud-first messaging",
    "data_platform_initiative": "Data platform, lakehouse, or data mesh initiatives",
    "high_intent_phrase_devops_toil": "DevOps pain, toil, burnout, or manual work complaints",
    "high_intent_phrase_production_fast": "Desire to ship faster or deploy more frequently",
    "devops_bottleneck_language": "Slow deployments, bottlenecks, or automation needs",
    "env_spinup_requests": "Ephemeral or dev environment provisioning needs",
    "idp_golden_path_initiative": "Internal developer platform or golden path discussions",
    "security_baseline_as_default": "DevSecOps, shift-left security, or zero-trust adoption",
    "vendor_consolidation_program": "Tool rationalization or vendor consolidation",
    "compliance_governance_messaging": "Data governance or regulatory compliance messaging",
    "finops_tool_eval": "Evaluating FinOps tools or cloud cost platforms",
    "recent_funding_event": "Funding round or investment announcement",
    "funding_stage_series_a": "Series A or seed funding round",
    "funding_stage_series_b_plus": "Series B or later funding round",
    "employee_growth_positive": "Headcount growth or team expansion",
    "company_news_mention": "General newsworthy company mention",
}


@dataclass
class TweetClassification:
    tweet_index: int
    signal_code: str  # "none" if irrelevant
    confidence: float
    reasoning: str
    is_decision_maker: bool
    author_role_guess: str


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You classify tweets about a company into buying signal categories for B2B SaaS sales intelligence.

Given tweets about a specific company, classify each tweet into exactly ONE of the signal codes below, or "none" if the tweet has no buying-signal relevance for DevOps, Platform Engineering, or FinOps tooling sales.

SIGNAL CODES:
{signal_list}

RULES:
- Use exactly one signal_code per tweet from the list above, or "none".
- Set confidence between 0.5 (marginal match) and 0.95 (very clear signal).
- Set is_decision_maker to true ONLY if the tweet author appears to be a CTO, VP, Director, Head of Engineering/Platform/Infrastructure/DevOps/Cloud, or similar technical leader based on their handle or tweet content.
- Provide a brief reasoning (one sentence) explaining the classification.
- Return ONLY a JSON array, no other text."""

_USER_PROMPT_HEADER = """Classify these tweets about {company_name} ({domain}):

"""


def build_classification_prompt(
    tweets: list[dict],
    company_name: str,
    domain: str,
) -> tuple[str, str]:
    """Build (system_prompt, user_prompt) for batch tweet classification."""
    signal_list = "\n".join(f"- {code}: {desc}" for code, desc in sorted(_SIGNAL_DESCRIPTIONS.items()))
    system = _SYSTEM_PROMPT.format(signal_list=signal_list)

    lines: list[str] = [_USER_PROMPT_HEADER.format(company_name=company_name, domain=domain)]
    for i, tweet in enumerate(tweets):
        author = tweet.get("author", tweet.get("screen_name", "unknown"))
        text = str(tweet.get("text", "")).replace("\n", " ").strip()[:400]
        lines.append(f'[{i}] @{author}: "{text}"')

    lines.append(
        "\nRespond with a JSON array:"
        ' [{"index": 0, "signal_code": "...", "confidence": 0.7, '
        '"reasoning": "...", "is_decision_maker": false, "author_role_guess": ""}]'
    )
    return system, "\n".join(lines)


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*\n?(.*?)\n?```", re.DOTALL)


def parse_classification_response(raw_text: str, tweet_count: int) -> list[TweetClassification]:
    """Parse LLM JSON response into TweetClassification objects.

    Handles markdown-fenced JSON, malformed responses, and invalid codes.
    """
    text = raw_text.strip()

    # Strip markdown code fences if present
    m = _JSON_BLOCK_RE.search(text)
    if m:
        text = m.group(1).strip()

    # Try direct JSON parse
    try:
        items = json.loads(text)
    except json.JSONDecodeError:
        # Fallback: try to find the JSON array in the text
        bracket_start = text.find("[")
        bracket_end = text.rfind("]")
        if bracket_start != -1 and bracket_end > bracket_start:
            try:
                items = json.loads(text[bracket_start : bracket_end + 1])
            except json.JSONDecodeError:
                logger.warning("twitter_classify: failed to parse LLM response as JSON")
                return []
        else:
            logger.warning("twitter_classify: no JSON array found in LLM response")
            return []

    if not isinstance(items, list):
        logger.warning("twitter_classify: LLM response is not a JSON array")
        return []

    results: list[TweetClassification] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        idx = int(item.get("index", -1))
        if idx < 0 or idx >= tweet_count:
            continue
        signal_code = str(item.get("signal_code", "none")).strip()
        if signal_code not in VALID_SIGNAL_CODES:
            signal_code = "none"
        confidence = float(item.get("confidence", 0.5))
        confidence = max(0.5, min(0.95, confidence))
        results.append(
            TweetClassification(
                tweet_index=idx,
                signal_code=signal_code,
                confidence=confidence,
                reasoning=str(item.get("reasoning", ""))[:200],
                is_decision_maker=bool(item.get("is_decision_maker", False)),
                author_role_guess=str(item.get("author_role_guess", ""))[:120],
            )
        )
    return results


# ---------------------------------------------------------------------------
# High-level batch classification
# ---------------------------------------------------------------------------


def classify_tweets_batch(
    llm_client,
    tweets: list[dict],
    company_name: str,
    domain: str,
) -> list[TweetClassification]:
    """Classify a batch of tweets using an LLM client.

    Args:
        llm_client: ResearchClient or MiniMaxClient from src.research.client
        tweets: list of dicts with at least 'text' key, optionally 'author'
        company_name: target company name
        domain: target company domain

    Returns:
        list of TweetClassification (may be shorter than input if parsing fails)
    """
    if not tweets:
        return []

    system_prompt, user_prompt = build_classification_prompt(tweets, company_name, domain)

    try:
        response = llm_client.research_company(system_prompt, user_prompt)
        classifications = parse_classification_response(response.raw_text, len(tweets))
        logger.info(
            "twitter_classify batch=%d classified=%d tokens_in=%d tokens_out=%d duration=%.1fs",
            len(tweets),
            len(classifications),
            response.input_tokens,
            response.output_tokens,
            response.duration_seconds,
        )
        return classifications
    except Exception as exc:
        logger.warning("twitter_classify: LLM call failed: %s", exc)
        return []

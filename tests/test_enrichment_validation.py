import pytest
from src.models import EnrichmentData

def test_enrichment_validation_defaults():
    # Empty dict should fill with defaults
    data = EnrichmentData.model_validate({})
    assert data.website == ""
    assert data.employees is None
    assert data.tech_stack == []

def test_enrichment_validation_partial():
    # Only some fields populated
    partial = {"website": "example.com", "employees": 50}
    data = EnrichmentData.model_validate(partial)
    assert data.website == "example.com"
    assert data.employees == 50
    assert data.industry == ""

def test_enrichment_validation_dump():
    data = EnrichmentData.model_validate({"website": "test.com"})
    dumped = data.model_dump()
    assert "website" in dumped
    assert "industry" in dumped
    assert dumped["website"] == "test.com"

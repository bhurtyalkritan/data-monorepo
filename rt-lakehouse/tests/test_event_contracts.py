import pytest
from datetime import datetime, timezone


@pytest.fixture
def sample_event():
    """Sample event for testing event contracts"""
    return {
        "event_id": "test-event-123",
        "user_id": "user-456",
        "session_id": "session-789",
        "event_type": "page_view",
        "ts": datetime.now(timezone.utc).isoformat(),
        "product_id": "product-abc",
        "category": "electronics",
        "price": 29.99,
        "quantity": 1,
        "currency": "USD",
        "country": "US",
        "device": "mobile",
        "campaign": "spring_sale",
        "referrer": "google"
    }


def test_event_basic_keys(sample_event):
    """Test that events have required basic keys"""
    assert all(k in sample_event for k in ["event_id", "user_id", "event_type", "ts"])


def test_event_types(sample_event):
    """Test that event types are valid"""
    valid_types = ["page_view", "add_to_cart", "purchase"]
    assert sample_event["event_type"] in valid_types


def test_event_id_format(sample_event):
    """Test that event_id is a non-empty string"""
    assert isinstance(sample_event["event_id"], str)
    assert len(sample_event["event_id"]) > 0


def test_user_id_format(sample_event):
    """Test that user_id is a non-empty string"""
    assert isinstance(sample_event["user_id"], str)
    assert len(sample_event["user_id"]) > 0

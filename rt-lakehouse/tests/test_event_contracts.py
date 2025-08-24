def test_event_basic_keys(sample_event):
    assert all(k in sample_event for k in ["event_id","user_id","event_type","ts"])

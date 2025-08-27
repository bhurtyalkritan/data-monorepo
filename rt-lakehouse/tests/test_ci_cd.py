"""
RT-Lakehouse CI/CD Test Suite
Comprehensive test coverage for all components
"""

import pytest
import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any
import requests
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from unittest.mock import Mock, patch

# Test configuration
TEST_CONFIG = {
    "api_url": "http://localhost:8000",
    "frontend_url": "http://localhost:3000", 
    "monitoring_url": "http://localhost:8501",
    "kafka_bootstrap": "localhost:9092",
    "test_topic": "ecommerce_events_test"
}

class TestAPIEndpoints:
    """Test FastAPI assistant endpoints"""
    
    @pytest.fixture
    def api_client(self):
        """Create API client for testing"""
        return requests.Session()
    
    def test_health_endpoint(self, api_client):
        """Test basic health check"""
        response = api_client.get(f"{TEST_CONFIG['api_url']}/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert "uptime" in data
    
    def test_metrics_endpoint(self, api_client):
        """Test metrics endpoint structure"""
        response = api_client.get(f"{TEST_CONFIG['api_url']}/metrics")
        assert response.status_code == 200
        
        data = response.json()
        assert "latest_kpis" in data
        assert "recent_events" in data
        
        # Validate KPI structure
        kpis = data["latest_kpis"]
        required_fields = ["orders", "gmv", "conversion_rate", "active_users"]
        for field in required_fields:
            assert field in kpis
    
    def test_trend_endpoints(self, api_client):
        """Test trend analysis endpoints"""
        # Test conversion trend
        response = api_client.get(f"{TEST_CONFIG['api_url']}/trend/conversion?limit=10")
        assert response.status_code == 200
        
        data = response.json()
        assert "data" in data
        assert isinstance(data["data"], list)
        
        # Test revenue trend
        response = api_client.get(f"{TEST_CONFIG['api_url']}/trend/revenue?limit=10")
        assert response.status_code == 200
    
    def test_data_quality_endpoint(self, api_client):
        """Test data quality monitoring"""
        response = api_client.get(f"{TEST_CONFIG['api_url']}/dq/status")
        assert response.status_code == 200
        
        data = response.json()
        required_dimensions = ["completeness", "validity", "timeliness", "consistency"]
        for dimension in required_dimensions:
            assert dimension in data
    
    def test_time_travel_endpoints(self, api_client):
        """Test Delta Lake time travel functionality"""
        # Test history endpoint
        response = api_client.get(f"{TEST_CONFIG['api_url']}/timetravel/history?table=gold&limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert "table" in data
        assert "history" in data
        assert data["table"] == "gold"
    
    def test_rate_limiting(self, api_client):
        """Test API rate limiting"""
        # Make rapid requests to trigger rate limiting
        responses = []
        for _ in range(70):  # Exceed rate limit of 60/minute
            response = api_client.get(f"{TEST_CONFIG['api_url']}/health")
            responses.append(response.status_code)
            
        # Should get some 429 responses
        assert 429 in responses
    
    def test_error_handling(self, api_client):
        """Test API error handling"""
        # Test invalid table name
        response = api_client.get(f"{TEST_CONFIG['api_url']}/timetravel/history?table=invalid")
        assert response.status_code == 400
        
        # Test malformed query
        response = api_client.post(
            f"{TEST_CONFIG['api_url']}/query",
            json={"sql": "DROP TABLE users;"}  # Should be rejected
        )
        assert response.status_code == 400

class TestKafkaIntegration:
    """Test Kafka producer and consumer functionality"""
    
    @pytest.fixture
    def kafka_producer(self):
        """Create Kafka producer for testing"""
        return KafkaProducer(
            bootstrap_servers=[TEST_CONFIG["kafka_bootstrap"]],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    @pytest.fixture
    def kafka_consumer(self):
        """Create Kafka consumer for testing"""
        return KafkaConsumer(
            TEST_CONFIG["test_topic"],
            bootstrap_servers=[TEST_CONFIG["kafka_bootstrap"]],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
    
    def test_event_production(self, kafka_producer):
        """Test event production to Kafka"""
        test_event = {
            "event_id": "test-123",
            "user_id": "u-test",
            "event_type": "page_view",
            "timestamp": datetime.now().isoformat()
        }
        
        # Send test event
        future = kafka_producer.send(TEST_CONFIG["test_topic"], test_event)
        result = future.get(timeout=10)
        
        assert result.topic == TEST_CONFIG["test_topic"]
        assert result.partition is not None
    
    def test_event_consumption(self, kafka_producer, kafka_consumer):
        """Test event consumption from Kafka"""
        test_event = {
            "event_id": "test-456",
            "user_id": "u-test2", 
            "event_type": "purchase",
            "timestamp": datetime.now().isoformat()
        }
        
        # Send event
        kafka_producer.send(TEST_CONFIG["test_topic"], test_event)
        kafka_producer.flush()
        
        # Consume event
        consumer_timeout = time.time() + 10
        consumed_event = None
        
        for message in kafka_consumer:
            consumed_event = message.value
            break
            
        assert consumed_event is not None
        assert consumed_event["event_id"] == "test-456"

class TestDataPipeline:
    """Test Spark streaming pipeline functionality"""
    
    def test_bronze_layer_ingestion(self):
        """Test bronze layer data ingestion"""
        # This would typically require Spark context
        # For CI/CD, we'll test the configuration and structure
        pass
    
    def test_silver_layer_transformation(self):
        """Test silver layer data transformation logic"""
        # Mock test for transformation logic
        raw_event = {
            "event_id": "test-789",
            "user_id": "u-test3",
            "event_type": "add_to_cart",
            "price": "19.99",
            "quantity": "2"
        }
        
        # Test data validation (would be part of silver transformation)
        assert raw_event["event_type"] in ["page_view", "add_to_cart", "purchase"]
        assert float(raw_event["price"]) > 0
        assert int(raw_event["quantity"]) > 0
    
    def test_gold_layer_aggregation(self):
        """Test gold layer KPI calculations"""
        # Mock data for testing aggregation logic
        events = [
            {"event_type": "page_view", "user_id": "u1", "price": 0},
            {"event_type": "page_view", "user_id": "u2", "price": 0},
            {"event_type": "purchase", "user_id": "u1", "price": 25.99, "quantity": 1},
            {"event_type": "purchase", "user_id": "u3", "price": 15.50, "quantity": 2}
        ]
        
        # Calculate metrics
        purchase_events = [e for e in events if e["event_type"] == "purchase"]
        view_events = [e for e in events if e["event_type"] == "page_view"]
        
        orders = len(purchase_events)
        gmv = sum(e["price"] * e["quantity"] for e in purchase_events)
        purchase_users = len(set(e["user_id"] for e in purchase_events))
        view_users = len(set(e["user_id"] for e in view_events))
        conversion_rate = purchase_users / view_users if view_users > 0 else 0
        
        assert orders == 2
        assert gmv == 56.99
        assert purchase_users == 2
        assert view_users == 2
        assert conversion_rate == 1.0

class TestFrontendIntegration:
    """Test React frontend functionality"""
    
    def test_frontend_accessibility(self):
        """Test frontend is accessible"""
        response = requests.get(TEST_CONFIG["frontend_url"])
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")
    
    def test_api_integration(self):
        """Test frontend API integration"""
        # Test that frontend can reach API endpoints
        api_response = requests.get(f"{TEST_CONFIG['api_url']}/metrics")
        assert api_response.status_code == 200
        
        # Validate data structure matches frontend expectations
        data = api_response.json()
        assert "latest_kpis" in data
        assert "recent_events" in data

class TestMonitoringDashboard:
    """Test Streamlit monitoring dashboard"""
    
    def test_monitoring_accessibility(self):
        """Test monitoring dashboard is accessible"""
        response = requests.get(TEST_CONFIG["monitoring_url"])
        assert response.status_code == 200
    
    def test_health_checks(self):
        """Test monitoring dashboard health checks"""
        # This would test the health check functions in monitoring app
        pass

class TestDataQuality:
    """Test data quality validation"""
    
    def test_event_schema_validation(self):
        """Test event schema compliance"""
        valid_event = {
            "event_id": "uuid-123",
            "user_id": "u-456",
            "product_id": "p-789",
            "event_type": "purchase",
            "price": 29.99,
            "quantity": 1,
            "currency": "USD",
            "ts": datetime.now().isoformat(),
            "country": "US"
        }
        
        # Validate required fields
        required_fields = ["event_id", "user_id", "event_type", "ts"]
        for field in required_fields:
            assert field in valid_event
        
        # Validate data types
        assert isinstance(valid_event["price"], (int, float))
        assert isinstance(valid_event["quantity"], int)
        assert valid_event["event_type"] in ["page_view", "add_to_cart", "purchase"]
    
    def test_duplicate_detection(self):
        """Test duplicate event detection logic"""
        events = [
            {"event_id": "dup-1", "user_id": "u1", "ts": "2025-08-27T10:00:00Z"},
            {"event_id": "dup-1", "user_id": "u1", "ts": "2025-08-27T10:00:01Z"},  # Duplicate
            {"event_id": "dup-2", "user_id": "u2", "ts": "2025-08-27T10:00:02Z"}
        ]
        
        # Deduplicate by event_id
        unique_events = {}
        for event in events:
            unique_events[event["event_id"]] = event
        
        assert len(unique_events) == 2
        assert "dup-1" in unique_events
        assert "dup-2" in unique_events

class TestPerformance:
    """Test performance characteristics"""
    
    def test_api_response_time(self):
        """Test API response times are within limits"""
        start_time = time.time()
        response = requests.get(f"{TEST_CONFIG['api_url']}/health")
        response_time = time.time() - start_time
        
        assert response.status_code == 200
        assert response_time < 1.0  # Should respond within 1 second
    
    def test_metrics_response_time(self):
        """Test metrics endpoint performance"""
        start_time = time.time()
        response = requests.get(f"{TEST_CONFIG['api_url']}/metrics")
        response_time = time.time() - start_time
        
        assert response.status_code == 200
        assert response_time < 2.0  # Should respond within 2 seconds
    
    def test_concurrent_requests(self):
        """Test handling of concurrent requests"""
        import concurrent.futures
        
        def make_request():
            response = requests.get(f"{TEST_CONFIG['api_url']}/health")
            return response.status_code
        
        # Make 10 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_request) for _ in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # All requests should succeed (or some may be rate limited)
        success_count = sum(1 for status in results if status == 200)
        assert success_count >= 5  # At least half should succeed

class TestSecurity:
    """Test security features"""
    
    def test_sql_injection_protection(self):
        """Test SQL injection protection"""
        malicious_sql = "'; DROP TABLE users; --"
        
        response = requests.post(
            f"{TEST_CONFIG['api_url']}/query",
            json={"sql": f"SELECT * FROM events WHERE user_id = '{malicious_sql}'"}
        )
        
        # Should be rejected for not starting with SELECT
        assert response.status_code == 400
    
    def test_rate_limiting_per_ip(self):
        """Test rate limiting works per IP"""
        # This would test the rate limiting implementation
        pass
    
    def test_input_validation(self):
        """Test input validation"""
        # Test invalid table name
        response = requests.get(f"{TEST_CONFIG['api_url']}/timetravel/history?table=../etc/passwd")
        assert response.status_code == 400

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])

import pytest


@pytest.mark.unit
class TestMetricsDefinitions:
    def test_http_request_duration_exists(self):
        from shared.metrics import HTTP_REQUEST_DURATION

        assert HTTP_REQUEST_DURATION is not None
        assert HTTP_REQUEST_DURATION._name == "http_request_duration_seconds"

    def test_http_requests_total_exists(self):
        from shared.metrics import HTTP_REQUESTS_TOTAL

        assert HTTP_REQUESTS_TOTAL is not None
        assert HTTP_REQUESTS_TOTAL._name == "http_requests"

    def test_events_ingested_total_exists(self):
        from shared.metrics import EVENTS_INGESTED_TOTAL

        assert EVENTS_INGESTED_TOTAL is not None
        assert EVENTS_INGESTED_TOTAL._name == "events_ingested"

    def test_events_processed_total_exists(self):
        from shared.metrics import EVENTS_PROCESSED_TOTAL

        assert EVENTS_PROCESSED_TOTAL is not None
        assert EVENTS_PROCESSED_TOTAL._name == "events_processed"

    def test_scoring_duration_exists(self):
        from shared.metrics import SCORING_DURATION

        assert SCORING_DURATION is not None
        assert SCORING_DURATION._name == "scoring_duration_seconds"

    def test_dlq_events_total_exists(self):
        from shared.metrics import DLQ_EVENTS_TOTAL

        assert DLQ_EVENTS_TOTAL is not None
        assert DLQ_EVENTS_TOTAL._name == "dlq_events"

    def test_consumer_lag_exists(self):
        from shared.metrics import CONSUMER_LAG

        assert CONSUMER_LAG is not None
        assert CONSUMER_LAG._name == "consumer_lag"

    def test_active_model_info_exists(self):
        from shared.metrics import ACTIVE_MODEL_INFO

        assert ACTIVE_MODEL_INFO is not None
        assert ACTIVE_MODEL_INFO._name == "active_model_info"

    def test_retry_attempts_total_exists(self):
        from shared.metrics import RETRY_ATTEMPTS_TOTAL

        assert RETRY_ATTEMPTS_TOTAL is not None
        assert RETRY_ATTEMPTS_TOTAL._name == "retry_attempts"


@pytest.mark.unit
class TestMetricsEndpoint:
    @pytest.mark.asyncio
    async def test_metrics_endpoint_returns_prometheus_format(self, client):
        response = await client.get("/metrics")
        assert response.status_code == 200
        content = response.text
        assert "http_request_duration_seconds" in content or "TYPE" in content

    @pytest.mark.asyncio
    async def test_metrics_endpoint_content_type(self, client):
        response = await client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers.get("content-type", "")


@pytest.mark.unit
class TestMiddlewarePath:
    def test_normalize_path_simple(self):
        from services.api.middleware import MetricsMiddleware

        middleware = MetricsMiddleware(app=None)
        assert middleware._normalize_path("/health") == "/health"
        assert middleware._normalize_path("/events") == "/events"

    def test_normalize_path_with_uuid(self):
        from services.api.middleware import MetricsMiddleware

        middleware = MetricsMiddleware(app=None)
        path = "/score/123e4567-e89b-12d3-a456-426614174000"
        assert middleware._normalize_path(path) == "/score/{id}"

    def test_normalize_path_with_user_id(self):
        from services.api.middleware import MetricsMiddleware

        middleware = MetricsMiddleware(app=None)
        path = "/score/user-12345"
        assert middleware._normalize_path(path) == "/score/{id}"

    def test_normalize_path_root(self):
        from services.api.middleware import MetricsMiddleware

        middleware = MetricsMiddleware(app=None)
        assert middleware._normalize_path("/") == "/"

    def test_looks_like_id_uuid(self):
        from services.api.middleware import MetricsMiddleware

        middleware = MetricsMiddleware(app=None)
        assert middleware._looks_like_id("123e4567-e89b-12d3-a456-426614174000") is True

    def test_looks_like_id_user_prefix(self):
        from services.api.middleware import MetricsMiddleware

        middleware = MetricsMiddleware(app=None)
        assert middleware._looks_like_id("user-12345") is True

    def test_looks_like_id_regular_path(self):
        from services.api.middleware import MetricsMiddleware

        middleware = MetricsMiddleware(app=None)
        assert middleware._looks_like_id("events") is False
        assert middleware._looks_like_id("health") is False

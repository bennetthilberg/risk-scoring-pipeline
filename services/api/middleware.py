"""FastAPI middleware for metrics collection."""

import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from shared.metrics import HTTP_REQUEST_DURATION, HTTP_REQUESTS_TOTAL


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path == "/metrics":
            return await call_next(request)

        start_time = time.perf_counter()

        response = await call_next(request)

        duration = time.perf_counter() - start_time

        endpoint = self._normalize_path(request.url.path)
        method = request.method
        status = str(response.status_code)

        HTTP_REQUEST_DURATION.labels(
            method=method,
            endpoint=endpoint,
            status=status,
        ).observe(duration)

        HTTP_REQUESTS_TOTAL.labels(
            method=method,
            endpoint=endpoint,
            status=status,
        ).inc()

        return response

    def _normalize_path(self, path: str) -> str:
        parts = path.strip("/").split("/")
        normalized = []
        for part in parts:
            if self._looks_like_id(part):
                normalized.append("{id}")
            else:
                normalized.append(part)
        return "/" + "/".join(normalized) if normalized else "/"

    def _looks_like_id(self, part: str) -> bool:
        if len(part) == 36 and part.count("-") == 4:
            return True
        return part.startswith("user-")

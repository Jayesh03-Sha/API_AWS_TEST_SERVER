from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple, Any
import logging
import time

logger = logging.getLogger(__name__)


class BaseProvider(ABC):
    """Abstract base class for all insurance providers"""

    def __init__(self, api_key: str = None, base_url: str = None):
        self.api_key = api_key
        self.base_url = base_url
        self.provider_name = None
        self.timeout = 10  # seconds (used if request_timeout is not set)
        # Optional (connect_timeout, read_timeout); better for large JSON bodies to remote APIs.
        self.request_timeout: Optional[Tuple[float, float]] = None
        # Retries for transport errors (connection reset, timeouts while reading, etc.)
        self.http_max_retries = 1
        self.api_logger = logging.getLogger("api_providers")
        self._last_transport_error: Optional[str] = None

    @abstractmethod
    def get_quote(self, data: Dict) -> Optional[Dict]:
        """
        Get a quote from the provider.

        Args:
            data: Dictionary containing quote parameters

        Returns:
            Normalized quote dictionary or None if failed
        """
        pass

    @abstractmethod
    def normalize(self, response_data: Dict) -> Dict:
        """
        Normalize provider API response to standard format.

        Args:
            response_data: Raw response from provider API

        Returns:
            Standardized quote dictionary
        """
        pass

    def _make_request(self, method: str = "POST", endpoint: str = "", **kwargs) -> Tuple[Optional[Dict[Any, Any]], int]:
        """
        Make HTTP request to provider API.

        Returns:
            (json_dict_or_none, response_time_ms) — second value is 0 on hard transport failure.
        """
        import requests
        import urllib.parse

        url = urllib.parse.urljoin(self.base_url, endpoint) if self.base_url else endpoint
        timeout_param: Any = self.request_timeout if self.request_timeout is not None else self.timeout
        max_retries = max(1, int(getattr(self, "http_max_retries", 1)))
        saved_kwargs = dict(kwargs)

        self._last_transport_error = None

        for attempt in range(max_retries):
            # Fresh kwargs each attempt (headers must be pop-able each time).
            # Shallow copy only — we only `pop("headers")`; `json` dict is not mutated.
            kw = dict(saved_kwargs)
            try:
                start_time = time.time()

                headers = kw.pop("headers", {}) or {}
                if self.api_key and not headers.get("Authorization"):
                    headers["Authorization"] = f"Bearer {self.api_key}"

                json_payload = kw.get("json", {})
                params = kw.get("params", {})

                self.api_logger.debug(
                    f"REQUEST | Provider: {self.provider_name} | Method: {method} | URL: {url}\n"
                    f"HEADERS: {headers}\n"
                    f"PAYLOAD: {json_payload or params}"
                )

                if method.upper() == "POST":
                    response = requests.post(
                        url,
                        headers=headers,
                        timeout=timeout_param,
                        **kw,
                    )
                else:
                    response = requests.get(
                        url,
                        headers=headers,
                        timeout=timeout_param,
                        **kw,
                    )

                response_time = int((time.time() - start_time) * 1000)

                self.api_logger.debug(
                    f"RESPONSE | Provider: {self.provider_name} | Status: {response.status_code} | "
                    f"Time: {response_time}ms\n"
                    f"BODY: {response.text[:1000]}{'...' if len(response.text) > 1000 else ''}"
                )

                if response.status_code == 200:
                    self._last_transport_error = None
                    try:
                        return response.json(), response_time
                    except Exception:
                        return {"raw_response": response.text}, response_time

                # Non-200: still return body so provider can surface proper error details upstream.
                preview = response.text[:4000] if response.text else ""
                self.api_logger.warning(
                    f"{self.provider_name} HTTP {response.status_code} | URL: {url}\nBODY: {preview}"
                )
                try:
                    body = response.json()
                except Exception:
                    body = {"raw_response": response.text}
                if isinstance(body, dict):
                    body.setdefault("_http_status", response.status_code)
                    body.setdefault("_url", url)
                return body, response_time

            except requests.exceptions.Timeout as e:
                self._last_transport_error = f"timeout:{e}"
                self.api_logger.warning(
                    f"{self.provider_name} request timeout (attempt {attempt + 1}/{max_retries}) | URL: {url} | {e}"
                )
                if attempt < max_retries - 1:
                    delay = 0.5 * (2**attempt)
                    time.sleep(delay)
                    continue
                self.api_logger.error(f"{self.provider_name} request timeout | URL: {url}")
                return None, int(self.timeout * 1000)

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as e:
                self._last_transport_error = str(e)[:800]
                self.api_logger.warning(
                    f"{self.provider_name} transport error (attempt {attempt + 1}/{max_retries}) | "
                    f"URL: {url} | {e!r}"
                )
                if attempt < max_retries - 1:
                    delay = 0.5 * (2**attempt)
                    time.sleep(delay)
                    continue
                self.api_logger.error(
                    f"{self.provider_name} connection error | URL: {url} | {e}",
                    exc_info=True,
                )
                return None, 0

            except Exception as e:
                self._last_transport_error = str(e)[:800]
                self.api_logger.error(
                    f"{self.provider_name} request error | URL: {url} | {e}",
                    exc_info=True,
                )
                return None, 0

        return None, 0

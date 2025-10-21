from __future__ import annotations
import time
import requests
from requests.adapters import HTTPAdapter, Retry


def make_session(user_agent: str, retries: int = 3, backoff_factor: float = 0.5, timeout: int = 60) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.request_timeout = timeout
    return session


def get(session: requests.Session, url: str, timeout: int | None = None) -> requests.Response:
    t = timeout if timeout is not None else getattr(session, "request_timeout", 60)
    resp = session.get(url, timeout=t)
    resp.raise_for_status()
    # respeitar politeness básica
    time.sleep(0.5)
    return resp

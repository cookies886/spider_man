"""HTTP response wrapper carrying its request and business seed."""
from __future__ import annotations


class Response:
    def __init__(self, *, request, response, elapsed=None):
        self.request, self.response = request, response
        self.seed = request.seed
        self.elapsed = elapsed if elapsed is not None else response.elapsed.total_seconds()

    @property
    def status_code(self):
        return self.response.status_code

    @property
    def url(self):
        return self.response.url

    @property
    def headers(self):
        return self.response.headers

    @property
    def text(self):
        return self.response.text

    @property
    def content(self):
        return self.response.content

    def json(self, **kwargs):
        return self.response.json(**kwargs)

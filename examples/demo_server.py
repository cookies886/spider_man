"""Local HTTP source with deterministic three-page synthetic data."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from urllib.parse import parse_qs, urlsplit


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        query = parse_qs(urlsplit(self.path).query)
        try:
            page = int(query.get("page", ["1"])[0])
            user = query["user_id"][0]
            if not 1 <= page <= 3:
                raise ValueError("page out of range")
        except (ValueError, KeyError):
            self.send_error(400)
            return
        content = json.dumps({"posts": [{"user_id": user, "post_id": page * 10 + n,
                                         "title": f"demo post {page}-{n}"} for n in range(2)],
                              "next_page": page + 1 if page < 3 else None}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 18080), Handler)
    print("Synthetic source listening on http://127.0.0.1:18080", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler


class Handler(BaseHTTPRequestHandler):
    def do_GET(self, *args, **kwargs):
        self.send_response(200)
        self.send_header('Content-Type', 'application/binary')
        self.end_headers()
        self.wfile.write(b"xyz")

class ThreadedLocalServer(threading.Thread):
    def __init__(self, address=('', 8000), handler_class=Handler):
        super().__init__(daemon=True)
        self.address = address
        self._handler_class = handler_class
        self._server = None
        self._server_ready = threading.Event()

    def start(self):
        super().start()
        self._server_ready.wait()

    def run(self):
        self._server = HTTPServer(self.address, self._handler_class)
        self._server_ready.set()
        self._server.serve_forever()

    def shutdown(self):
        if self._server is not None:
            self._server.shutdown()
        self.join(timeout=5)
        assert not self.is_alive(), "Failed to join thread"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.shutdown()

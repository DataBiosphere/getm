import socket
import threading
from contextlib import closing
from http.server import HTTPServer, BaseHTTPRequestHandler


class ThreadedLocalServer(threading.Thread):
    def __init__(self, handler_class: BaseHTTPRequestHandler):
        super().__init__(daemon=True)
        self.port = _get_port()
        self.host = f"http://localhost:{self.port}"
        self._handler_class = handler_class
        self._server = None
        self._server_ready = threading.Event()

    def start(self):
        super().start()
        self._server_ready.wait()

    def run(self):
        self._server = HTTPServer(('', self.port), self._handler_class)
        self._server_ready.set()
        self._server.serve_forever()

    def shutdown(self):
        if self._server is not None:
            self._server.shutdown()
        self.join(timeout=5)
        assert not self.is_alive(), "Failed to join thread"

    def __enter__(self) -> str:
        self.start()
        return self.host

    def __exit__(self, *args):
        self.shutdown()

class SilentHandler(BaseHTTPRequestHandler):
    def log_message(self, *args, **kwargs):
        pass

def _get_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(('', 0))
        return sock.getsockname()[1]

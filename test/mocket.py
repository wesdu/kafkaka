import socket
from contextlib import contextmanager
from mock import patch

from tornado import gen
from tornado import iostream
from StringIO import StringIO

@contextmanager
def mocket(*sock_params):
    m = MockStream(*sock_params)
    try:
        p = patch("tornado.iostream.IOStream.__new__", return_value=m)
        p.start()
        yield m
    finally:
        p.stop()
        # if self.sock and self.sock.socket:
        #     self.sock.close()


class MockStream(object):
    def __init__(self, *sock_params):
        if sock_params:
            sock = socket.socket(*sock_params)
            self.sock = iostream.IOStream(sock)
        else:
            self.sock = None
        self._closed = True
        self.transcript = []
        self.response_buffer = StringIO()

    def add_response(self, script):
        assert not self.sock, "Adding responses won't work if we are proxying"
        # rather than using write, we want to add this response to the end
        # of the current buffer so that subsequent read()s will work
        self.response_buffer.buf += script
        self.response_buffer.len += len(script)

    def get_transcript(self):
        return self.transcript

    @gen.coroutine
    def connect(self, *a, **kw):
        if self.sock and self._closed:
            yield self.sock.connect(*a, **kw)
        self._closed = False

    @gen.coroutine
    def close(self):
        yield self.sock.close()
        self._closed = True

    @gen.coroutine
    def read_bytes(self, chunk_size):
        if self.sock:
            res = yield self.sock.read_bytes(chunk_size)
        else:
            res = self.response_buffer.read(chunk_size)
        self.transcript.append(("read", chunk_size, res))
        raise gen.Return(res)

    @gen.coroutine
    def write(self, msg):
        self.transcript.append(("write", len(msg), msg))
        if self.sock:
            yield self.sock.write(msg)

    def closed(self):
        return self.sock.closed() if self.sock else self._closed

#!/usr/bin/python
"""Minimal asyncio based TCP communications module."""

import asyncio
import pickle
from time import time as _time

from .debugging import ModuleLogger, DebugContents, bacpypes_debugging
from .core import deferred
from .comm import PDU, Client, Server
from .comm import ServiceAccessPoint, ApplicationServiceElement

_debug = 0
_log = ModuleLogger(globals())

REBIND_SLEEP_INTERVAL = 2.0
CONNECT_TIMEOUT = 30.0

# ------------------------------------------------------------------------------
#   TCP Client
# ------------------------------------------------------------------------------

@bacpypes_debugging
class TCPClient(Client):
    def __init__(self, peer, loop=None):
        if _debug:
            TCPClient._debug("__init__ %r", peer)
        self.peer = peer
        self.loop = loop or asyncio.get_event_loop()
        self.reader = None
        self.writer = None

    async def connect(self):
        if _debug:
            TCPClient._debug("connect")
        self.reader, self.writer = await asyncio.open_connection(*self.peer)

    async def send(self, pdu):
        if _debug:
            TCPClient._debug("send %r", pdu)
        if not self.writer:
            await self.connect()
        self.writer.write(pdu.pduData)
        await self.writer.drain()

    async def receive(self):
        if _debug:
            TCPClient._debug("receive")
        data = await self.reader.read(65536)
        return PDU(data, source=self.peer)

    def close(self):
        if self.writer:
            self.writer.close()
            self.writer = None
            self.reader = None

# ------------------------------------------------------------------------------
#   TCP Server
# ------------------------------------------------------------------------------

@bacpypes_debugging
class TCPServer(Server, ServiceAccessPoint, asyncio.Protocol):
    def __init__(self, address, cid=None, sapID=None, loop=None):
        if _debug:
            TCPServer._debug("__init__ %r", address)
        Server.__init__(self, cid)
        ServiceAccessPoint.__init__(self, sapID)
        self.address = address
        self.loop = loop or asyncio.get_event_loop()
        self.transport = None

        coro = self.loop.create_server(lambda: self, *address)
        self.server = self.loop.run_until_complete(coro)

    def connection_made(self, transport):
        if _debug:
            TCPServer._debug("connection_made")
        self.transport = transport

    def data_received(self, data):
        if _debug:
            TCPServer._debug("data_received %r", len(data))
        pdu = PDU(data)
        if self.serviceElement:
            self.sap_indication(pdu)

    def send_pdu(self, pdu):
        if _debug:
            TCPServer._debug("send_pdu %r", pdu)
        if self.transport:
            self.transport.write(pdu.pduData)

    def close(self):
        if self.transport:
            self.transport.close()
            self.transport = None
        if self.server:
            self.server.close()
            self.loop.run_until_complete(self.server.wait_closed())
            self.server = None

# ------------------------------------------------------------------------------
#   Placeholder Director and Helper Classes
# ------------------------------------------------------------------------------

class StreamToPacket(Client, Server):
    def __init__(self, fn, cid=None, sid=None):
        Client.__init__(self, cid)
        Server.__init__(self, sid)
        self.packetFn = fn

    def indication(self, pdu):
        self.request(pdu)

    def confirmation(self, pdu):
        self.response(pdu)

class TCPClientDirector(Server, ServiceAccessPoint):
    def __init__(self, *args, **kwargs):
        Server.__init__(self)
        ServiceAccessPoint.__init__(self)

    def connect(self, address, reconnect=0):
        pass

    def indication(self, pdu):
        pass

class TCPServerDirector(Server, ServiceAccessPoint):
    def __init__(self, address=None, *args, **kwargs):
        Server.__init__(self)
        ServiceAccessPoint.__init__(self)
        self.address = address

    def indication(self, pdu):
        pass

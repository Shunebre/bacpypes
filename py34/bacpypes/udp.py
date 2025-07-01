#!/usr/bin/env python3

"""
UDP Communications Module
"""

import asyncio
import socket
import pickle
import queue

from time import time as _time

from .debugging import ModuleLogger, bacpypes_debugging

from .core import deferred
from .task import FunctionTask
from .comm import PDU, Server
from .comm import ServiceAccessPoint

# some debugging
_debug = 0
_log = ModuleLogger(globals())

#
#   UDPActor
#
#   Actors are helper objects for a director.  There is one actor for
#   each peer.
#

@bacpypes_debugging
class UDPActor:

    def __init__(self, director, peer):
        if _debug: UDPActor._debug("__init__ %r %r", director, peer)

        # keep track of the director
        self.director = director

        # associated with a peer
        self.peer = peer

        # add a timer
        self.timeout = director.timeout
        if self.timeout > 0:
            self.timer = FunctionTask(self.idle_timeout)
            self.timer.install_task(_time() + self.timeout)
        else:
            self.timer = None

        # tell the director this is a new actor
        self.director.add_actor(self)

    def idle_timeout(self):
        if _debug: UDPActor._debug("idle_timeout")

        # tell the director this is gone
        self.director.del_actor(self)

    def indication(self, pdu):
        if _debug: UDPActor._debug("indication %r", pdu)

        # reschedule the timer
        if self.timer:
            self.timer.install_task(_time() + self.timeout)

        # put it in the outbound queue for the director
        self.director.request.put(pdu)

    def response(self, pdu):
        if _debug: UDPActor._debug("response %r", pdu)

        # reschedule the timer
        if self.timer:
            self.timer.install_task(_time() + self.timeout)

        # process this as a response from the director
        self.director.response(pdu)

    def handle_error(self, error=None):
        if _debug: UDPActor._debug("handle_error %r", error)

        # pass along to the director
        if error is not None:
            self.director.actor_error(self, error)

#
#   UDPPickleActor
#

@bacpypes_debugging
class UDPPickleActor(UDPActor):

    def __init__(self, *args):
        if _debug: UDPPickleActor._debug("__init__ %r", args)
        UDPActor.__init__(self, *args)

    def indication(self, pdu):
        if _debug: UDPPickleActor._debug("indication %r", pdu)

        # pickle the data
        pdu.pduData = pickle.dumps(pdu.pduData)

        # continue as usual
        UDPActor.indication(self, pdu)

    def response(self, pdu):
        if _debug: UDPPickleActor._debug("response %r", pdu)

        # unpickle the data
        try:
            pdu.pduData = pickle.loads(pdu.pduData)
        except:
            UDPPickleActor._exception("pickle error")
            return

        # continue as usual
        UDPActor.response(self, pdu)

#
#   UDPDirector
#

@bacpypes_debugging
class UDPDirector(asyncio.DatagramProtocol, Server, ServiceAccessPoint):

    def __init__(self, address, timeout=0, reuse=False, actorClass=UDPActor, sid=None, sapID=None, loop=None):
        if _debug:
            UDPDirector._debug("__init__ %r timeout=%r reuse=%r actorClass=%r sid=%r sapID=%r", address, timeout, reuse, actorClass, sid, sapID)
        Server.__init__(self, sid)
        ServiceAccessPoint.__init__(self, sapID)

        if not issubclass(actorClass, UDPActor):
            raise TypeError("actorClass must be a subclass of UDPActor")
        self.actorClass = actorClass

        self.timeout = timeout
        self.address = address

        self.loop = loop or asyncio.get_event_loop()
        self.transport = None

        listen = self.loop.create_datagram_endpoint(lambda: self, local_addr=address)
        self.transport, _ = self.loop.run_until_complete(listen)

        if reuse and hasattr(self.transport, 'getsockname'):
            sock = self.transport.get_extra_info('socket')
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.request = queue.Queue()
        self.peers = {}

    def add_actor(self, actor):
        """Add an actor when a new one is connected."""
        if _debug: UDPDirector._debug("add_actor %r", actor)

        self.peers[actor.peer] = actor

        # tell the ASE there is a new client
        if self.serviceElement:
            self.sap_request(add_actor=actor)

    def del_actor(self, actor):
        """Remove an actor when the socket is closed."""
        if _debug: UDPDirector._debug("del_actor %r", actor)

        del self.peers[actor.peer]

        # tell the ASE the client has gone away
        if self.serviceElement:
            self.sap_request(del_actor=actor)

    def actor_error(self, actor, error):
        if _debug: UDPDirector._debug("actor_error %r %r", actor, error)

        # tell the ASE the actor had an error
        if self.serviceElement:
            self.sap_request(actor_error=actor, error=error)

    def get_actor(self, address):
        return self.peers.get(address, None)

    def handle_connect(self):
        if _debug: UDPDirector._debug("handle_connect")

    # asyncio callbacks

    def datagram_received(self, data, addr):
        if _debug:
            UDPDirector._debug("datagram_received %r from %r", len(data), addr)

        deferred(self._response, PDU(data, source=addr))

    def error_received(self, exc):
        if _debug:
            UDPDirector._debug("error_received %r", exc)
        self.handle_error(exc)

    def close_socket(self):
        """Close the socket."""
        if _debug: UDPDirector._debug("close_socket")

        if self.transport:
            self.transport.close()
            self.transport = None

    def handle_close(self):
        """Remove this from the monitor when it's closed."""
        if _debug: UDPDirector._debug("handle_close")

        if self.transport:
            self.transport.close()
            self.transport = None

    def connection_lost(self, exc):
        if _debug:
            UDPDirector._debug("connection_lost %r", exc)
        self.transport = None

    def handle_error(self, error=None):
        if _debug: UDPDirector._debug("handle_error %r", error)

    def indication(self, pdu):
        """Client requests are queued for delivery."""
        if _debug: UDPDirector._debug("indication %r", pdu)

        addr = pdu.pduDestination
        peer = self.peers.get(addr, None)
        if not peer:
            peer = self.actorClass(self, addr)
        peer.indication(pdu)
        if self.transport:
            self.transport.sendto(pdu.pduData, addr)

    def _response(self, pdu):
        """Incoming datagrams are routed through an actor."""
        if _debug: UDPDirector._debug("_response %r", pdu)

        # get the destination
        addr = pdu.pduSource

        # get the peer
        peer = self.peers.get(addr, None)
        if not peer:
            peer = self.actorClass(self, addr)

        # send the message
        peer.response(pdu)

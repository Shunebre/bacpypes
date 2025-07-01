#/usr/bin/python

"""
Event
"""

import asyncio

from .debugging import Logging, bacpypes_debugging, ModuleLogger

# some debugging
_debug = 0
_log = ModuleLogger(globals())

#
#   WaitableEvent
#
#   An instance of this class can be used like a Threading.Event, but will
#   break the asyncore.loop().
#

@bacpypes_debugging
class WaitableEvent(Logging):

    def __init__(self, loop=None):
        if _debug:
            WaitableEvent._debug("__init__ loop=%r", loop)
        self._loop = loop or asyncio.get_event_loop()
        self._event = asyncio.Event()

    def __del__(self):
        if _debug:
            WaitableEvent._debug("__del__")

    #----- file methods

    # asyncio based event does not expose file descriptor handlers

    #----- event methods

    def wait(self, timeout=None):
        try:
            self._loop.run_until_complete(
                asyncio.wait_for(self._event.wait(), timeout)
            )
            return True
        except asyncio.TimeoutError:
            return False

    def is_set(self):
        return self._event.is_set()

    def set(self):
        if _debug:
            WaitableEvent._debug("set")
        self._event.set()

    def clear(self):
        if _debug:
            WaitableEvent._debug("clear")
        self._event.clear()

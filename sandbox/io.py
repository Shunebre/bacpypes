#!/usr/bin/env python3

"""
IO Module
"""

import sys
import logging

from time import time as _time

import threading
import cPickle
from bisect import bisect_left
from collections import deque

from bacpypes.debugging import bacpypes_debugging, DebugContents, ModuleLogger

from bacpypes.core import deferred

from bacpypes.comm import PDU, Client, bind
from bacpypes.task import FunctionTask
from bacpypes.udp import UDPDirector

# some debugging
_debug = 0
_log = ModuleLogger(globals())
_commlog = logging.getLogger(__name__ + "._commlog")

#
#   IOCB States
#

IDLE = 0        # has not been submitted
PENDING = 1     # queued, waiting for processing
ACTIVE = 2      # being processed
COMPLETED = 3   # finished
ABORTED = 4     # finished in a bad way

_stateNames = {
    IDLE: 'IDLE',
    PENDING: 'PENDING',
    ACTIVE: 'ACTIVE',
    COMPLETED: 'COMPLETED',
    ABORTED: 'ABORTED',
    }

#
#   IOQController States
#

CTRL_IDLE = 0       # nothing happening
CTRL_ACTIVE = 1     # working on an iocb
CTRL_WAITING = 1    # waiting between iocb requests (throttled)

_ctrlStateNames = {
    CTRL_IDLE: 'IDLE',
    CTRL_ACTIVE: 'ACTIVE',
    CTRL_WAITING: 'WAITING',
    }

# dictionary of local controllers
_local_controllers = {}
_proxy_server = None

# special abort error
TimeoutError = RuntimeError("timeout")

#
#   _strftime
#

def _strftime():
    return "%011.6f" % (_time() % 3600,)

#
#   IOCB - Input Output Control Block
#

_identNext = 1
_identLock = threading.Lock()

@bacpypes_debugging
class IOCB(DebugContents):

    _debug_contents = \
        ( 'args', 'kwargs'
        , 'ioState', 'ioResponse-', 'ioError'
        , 'ioController', 'ioServerRef', 'ioControllerRef', 'ioClientID', 'ioClientAddr'
        , 'ioComplete', 'ioCallback+', 'ioQueue', 'ioPriority', 'ioTimeout'
        )

    def __init__(self, *args, **kwargs):
        global _identNext

        # lock the identity sequence number
        _identLock.acquire()

        # generate a unique identity for this block
        ioID = _identNext
        _identNext += 1

        # release the lock
        _identLock.release()

        # debugging postponed until ID acquired
        if _debug: IOCB._debug("__init__(%d) %r %r", ioID, args, kwargs)

        # save the ID
        self.ioID = ioID

        # save the request parameters
        self.args = args
        self.kwargs = kwargs

        # start with an idle request
        self.ioState = IDLE
        self.ioResponse = None
        self.ioError = None

        # blocks are bound to a controller
        self.ioController = None

        # blocks could reference a local or remote server
        self.ioServerRef = None
        self.ioControllerRef = None
        self.ioClientID = None
        self.ioClientAddr = None

        # each block gets a completion event
        self.ioComplete = threading.Event()
        self.ioComplete.clear()

        # applications can set a callback functions
        self.ioCallback = []

        # request is not currently queued
        self.ioQueue = None

        # extract the priority if it was given
        self.ioPriority = kwargs.get('_priority', 0)
        if '_priority' in kwargs:
            if _debug: IOCB._debug("    - ioPriority: %r", self.ioPriority)
            del kwargs['_priority']

        # request has no timeout
        self.ioTimeout = None

    def add_callback(self, fn, *args, **kwargs):
        """Pass a function to be called when IO is complete."""
        if _debug: IOCB._debug("add_callback(%d) %r %r %r", self.ioID, fn, args, kwargs)

        # store it
        self.ioCallback.append((fn, args, kwargs))

        # already complete?
        if self.ioComplete.isSet():
            self.trigger()

    def wait(self, *args):
        """Wait for the completion event to be set."""
        if _debug: IOCB._debug("wait(%d) %r", self.ioID, args)

        # waiting from a non-daemon thread could be trouble
        self.ioComplete.wait(*args)

    def trigger(self):
        """Set the event and make the callback."""
        if _debug: IOCB._debug("trigger(%d)", self.ioID)

        # if it's queued, remove it from its queue
        if self.ioQueue:
            if _debug: IOCB._debug("    - dequeue")
            self.ioQueue.remove(self)

        # if there's a timer, cancel it
        if self.ioTimeout:
            if _debug: IOCB._debug("    - cancel timeout")
            self.ioTimeout.suspend_task()

        # set the completion event
        self.ioComplete.set()

        # make the callback
        for fn, args, kwargs in self.ioCallback:
            if _debug: IOCB._debug("    - callback fn: %r %r %r", fn, args, kwargs)
            fn(self, *args, **kwargs)

    def complete(self, msg):
        """Called to complete a transaction, usually when process_io has
        shipped the IOCB off to some other thread or function."""
        if _debug: IOCB._debug("complete(%d) %r", self.ioID, msg)

        if self.ioController:
            # pass to controller
            self.ioController.complete_io(self, msg)
        else:
            # just fill in the data
            self.ioState = COMPLETED
            self.ioResponse = msg
            self.trigger()

    def abort(self, err):
        """Called by a client to abort a transaction."""
        if _debug: IOCB._debug("abort(%d) %r", self.ioID, err)

        if self.ioController:
            # pass to controller
            self.ioController.abort_io(self, err)
        elif self.ioState < COMPLETED:
            # just fill in the data
            self.ioState = ABORTED
            self.ioError = err
            self.trigger()

    def set_timeout(self, delay, err=TimeoutError):
        """Called to set a transaction timer."""
        if _debug: IOCB._debug("set_timeout(%d) %r err=%r", self.ioID, delay, err)

        # if one has already been created, cancel it
        if self.ioTimeout:
            self.ioTimeout.suspend_task()
        else:
            self.ioTimeout = FunctionTask(self.abort, err)

        # (re)schedule it
        self.ioTimeout.install_task(_time() + delay)

    def __repr__(self):
        xid = id(self)
        if (xid < 0): xid += (1 << 32)

        sname = self.__module__ + '.' + self.__class__.__name__
        desc = "(%d)" % (self.ioID,)

        return '<' + sname + desc + ' instance at 0x%08x' % (xid,) + '>'

#
#   IOChainMixIn
#

@bacpypes_debugging
class IOChainMixIn(DebugContents):

    _debugContents = ( 'ioChain++', )

    def __init__(self, iocb):
        if _debug: IOChainMixIn._debug("__init__ %r", iocb)

        # save a refence back to the iocb
        self.ioChain = iocb

        # set the callback to follow the chain
        self.add_callback(self.chain_callback)

        # if we're not chained, there's no notification to do
        if not self.ioChain:
            return

        # this object becomes its controller
        iocb.ioController = self

        # consider the parent active
        iocb.ioState = ACTIVE

        try:
            if _debug: IOChainMixIn._debug("    - encoding")

            # let the derived class set the args and kwargs
            self.Encode()

            if _debug: IOChainMixIn._debug("    - encode complete")
        except:
            # extract the error and abort the request
            err = sys.exc_info()[1]
            if _debug: IOChainMixIn._exception("    - encoding exception: %r", err)

            iocb.abort(err)

    def chain_callback(self, iocb):
        """Callback when this iocb completes."""
        if _debug: IOChainMixIn._debug("chain_callback %r", iocb)

        # if we're not chained, there's no notification to do
        if not self.ioChain:
            return

        # refer to the chained iocb
        iocb = self.ioChain

        try:
            if _debug: IOChainMixIn._debug("    - decoding")

            # let the derived class transform the data
            self.Decode()

            if _debug: IOChainMixIn._debug("    - decode complete")
        except:
            # extract the error and abort
            err = sys.exc_info()[1]
            if _debug: IOChainMixIn._exception("    - decoding exception: %r", err)

            iocb.ioState = ABORTED
            iocb.ioError = err

        # break the references
        self.ioChain = None
        iocb.ioController = None

        # notify the client
        iocb.trigger()

    def abort_io(self, iocb, err):
        """Forward the abort downstream."""
        if _debug: IOChainMixIn._debug("abort_io %r %r", iocb, err)

        # make sure we're being notified of an abort request from
        # the iocb we are chained from
        if iocb is not self.ioChain:
            raise RuntimeError("broken chain")

        # call my own abort(), which may forward it to a controller or
        # be overridden by IOGroup
        self.abort(err)

    def encode(self):
        """Hook to transform the request, called when this IOCB is
        chained."""
        if _debug: IOChainMixIn._debug("encode (pass)")

        # by default do nothing, the arguments have already been supplied

    def decode(self):
        """Hook to transform the response, called when this IOCB is
        completed."""
        if _debug: IOChainMixIn._debug("decode")

        # refer to the chained iocb
        iocb = self.ioChain

        # if this has completed successfully, pass it up
        if self.ioState == COMPLETED:
            if _debug: IOChainMixIn._debug("    - completed: %r", self.ioResponse)

            # change the state and transform the content
            iocb.ioState = COMPLETED
            iocb.ioResponse = self.ioResponse

        # if this aborted, pass that up too
        elif self.ioState == ABORTED:
            if _debug: IOChainMixIn._debug("    - aborted: %r", self.ioError)

            # change the state
            iocb.ioState = ABORTED
            iocb.ioError = self.ioError

        else:
            raise RuntimeError("invalid state: %d" % (self.ioState,))

#
#   IOChain
#

class IOChain(IOCB, IOChainMixIn):

    def __init__(self, chain, *args, **kwargs):
        """Initialize a chained control block."""
        if _debug: IOChain._debug("__init__ %r %r %r", chain, args, kwargs)

        # initialize IOCB part to pick up the ioID
        IOCB.__init__(self, *args, **kwargs)
        IOChainMixIn.__init__(self, chain)

#
#   IOGroup
#

@bacpypes_debugging
class IOGroup(IOCB, DebugContents):

    _debugContents = ('ioMembers',)

    def __init__(self):
        """Initialize a group."""
        if _debug: IOGroup._debug("__init__")
        IOCB.__init__(self)

        # start with an empty list of members
        self.ioMembers = []

        # start out being done.  When an IOCB is added to the 
        # group that is not already completed, this state will 
        # change to PENDING.
        self.ioState = COMPLETED
        self.ioComplete.set()

    def add(self, iocb):
        """Add an IOCB to the group, you can also add other groups."""
        if _debug: IOGroup._debug("Add %r", iocb)

        # add this to our members
        self.ioMembers.append(iocb)

        # assume all of our members have not completed yet
        self.ioState = PENDING
        self.ioComplete.clear()

        # when this completes, call back to the group.  If this
        # has already completed, it will trigger
        iocb.add_callback(self.group_callback)

    def group_callback(self, iocb):
        """Callback when a child iocb completes."""
        if _debug: IOGroup._debug("group_callback %r", iocb)

        # check all the members
        for iocb in self.ioMembers:
            if not iocb.ioComplete.isSet():
                if _debug: IOGroup._debug("    - waiting for child: %r", iocb)
                break
        else:
            if _debug: IOGroup._debug("    - all children complete")
            # everything complete
            self.ioState = COMPLETED
            self.trigger()

    def abort(self, err):
        """Called by a client to abort all of the member transactions.
        When the last pending member is aborted the group callback
        function will be called."""
        if _debug: IOGroup._debug("abort %r", err)

        # change the state to reflect that it was killed
        self.ioState = ABORTED
        self.ioError = err

        # abort all the members
        for iocb in self.ioMembers:
            iocb.abort(err)

        # notify the client
        self.trigger()

#
#   IOQueue - Input Output Queue
#

@bacpypes_debugging
class IOQueue:

    def __init__(self, name):
        if _debug: IOQueue._debug("__init__ %r", name)

        self.queue = []
        self.notempty = threading.Event()
        self.notempty.clear()

    def put(self, iocb):
        """Add an IOCB to a queue.  This is usually called by the function
        that filters requests and passes them out to the correct processing
        thread."""
        if _debug: IOQueue._debug("put %r", iocb)

        # requests should be pending before being queued
        if iocb.ioState != PENDING:
            raise RuntimeError("invalid state transition")

        # save that it might have been empty
        wasempty = not self.notempty.isSet()

        # add the request to the end of the list of iocb's at same priority
        priority = iocb.ioPriority
        item = (priority, iocb)
        self.queue.insert(bisect_left(self.queue, (priority+1,)), item)

        # point the iocb back to this queue
        iocb.ioQueue = self

        # set the event, queue is no longer empty
        self.notempty.set()

        return wasempty

    def get(self, block=1, delay=None):
        """Get a request from a queue, optionally block until a request
        is available."""
        if _debug: IOQueue._debug("get block=%r delay=%r", block, delay)

        # if the queue is empty and we do not block return None
        if not block and not self.notempty.isSet():
            return None

        # wait for something to be in the queue
        if delay:
            self.notempty.wait(delay)
            if not self.notempty.isSet():
                return None
        else:
            self.notempty.wait()

        # extract the first element
        priority, iocb = self.queue[0]
        del self.queue[0]
        iocb.ioQueue = None

        # if the queue is empty, clear the event
        qlen = len(self.queue)
        if not qlen:
            self.notempty.clear()

        # return the request
        return iocb

    def remove(self, iocb):
        """Remove a control block from the queue, called if the request
        is canceled/aborted."""
        if _debug: IOQueue._debug("remove %r", iocb)

        # remove the request from the queue
        for i, item in enumerate(self.queue):
            if iocb is item[1]:
                if _debug: IOQueue._debug("    - found at %d", i)
                del self.queue[i]

                # if the queue is empty, clear the event
                qlen = len(self.queue)
                if not qlen:
                    self.notempty.clear()

                break
        else:
            if _debug: IOQueue._debug("    - not found")

    def abort(self, err):
        """abort all of the control blocks in the queue."""
        if _debug: IOQueue._debug("abort %r", err)

        # send aborts to all of the members
        try:
            for iocb in self.queue:
                iocb.ioQueue = None
                iocb.abort(err)

            # flush the queue
            self.queue = []

            # the queue is now empty, clear the event
            self.notempty.clear()
        except ValueError:
            pass

#
#   IOController
#

@bacpypes_debugging
class IOController:

    def __init__(self, name=None):
        """Initialize a controller."""
        if _debug: IOController._debug("__init__ name=%r", name)

        # save the name
        self.name = name

        # register the name
        if name is not None:
            if name in _local_controllers:
                raise RuntimeError("already a local controller called '%s': %r" % (name, _local_controllers[name]))
            _local_controllers[name] = self

    def abort(self, err):
        """Abort all requests, no default implementation."""
        pass

    def request_io(self, iocb):
        """Called by a client to start processing a request."""
        if _debug: IOController._debug("request_io %r", iocb)

        # bind the iocb to this controller
        iocb.ioController = self

        try:
            # hopefully there won't be an error
            err = None

            # change the state
            iocb.ioState = PENDING

            # let derived class figure out how to process this
            self.process_io(iocb)
        except:
            # extract the error
            err = sys.exc_info()[1]

        # if there was an error, abort the request
        if err:
            self.abort_io(iocb, err)

    def process_io(self, iocb):
        """Figure out how to respond to this request.  This must be
        provided by the derived class."""
        raise NotImplementedError("IOController must implement process_io()")

    def active_io(self, iocb):
        """Called by a handler to notify the controller that a request is
        being processed."""
        if _debug: IOController._debug("active_io %r", iocb)

        # requests should be idle or pending before coming active
        if (iocb.ioState != IDLE) and (iocb.ioState != PENDING):
            raise RuntimeError("invalid state transition (currently %d)" % (iocb.ioState,))

        # change the state
        iocb.ioState = ACTIVE

    def complete_io(self, iocb, msg):
        """Called by a handler to return data to the client."""
        if _debug: IOController._debug("complete_io %r %r", iocb, msg)

        # if it completed, leave it alone
        if iocb.ioState == COMPLETED:
            pass

        # if it already aborted, leave it alone
        elif iocb.ioState == ABORTED:
            pass

        else:
            # change the state
            iocb.ioState = COMPLETED
            iocb.ioResponse = msg

            # notify the client
            iocb.trigger()

    def abort_io(self, iocb, err):
        """Called by a handler or a client to abort a transaction."""
        if _debug: IOController._debug("abort_io %r %r", iocb, err)

        # if it completed, leave it alone
        if iocb.ioState == COMPLETED:
            pass

        # if it already aborted, leave it alone
        elif iocb.ioState == ABORTED:
            pass

        else:
            # change the state
            iocb.ioState = ABORTED
            iocb.ioError = err

            # notify the client
            iocb.trigger()

#
#   IOQController
#

@bacpypes_debugging
class IOQController(IOController):

    wait_time = 0.0

    def __init__(self, name=None):
        """Initialize a queue controller."""
        if _debug: IOQController._debug("__init__ name=%r", name)

        # give ourselves a nice name
        if not name:
            name = self.__class__.__name__
        IOController.__init__(self, name)

        # start idle
        self.state = CTRL_IDLE

        # no active iocb
        self.active_iocb = None

        # create an IOQueue for iocb's requested when not idle
        self.ioQueue = IOQueue(str(name) + "/Queue")

    def abort(self, err):
        """Abort all pending requests."""
        if _debug: IOQController._debug("abort %r", err)

        if (self.state == CTRL_IDLE):
            if _debug: IOQController._debug("    - idle")
            return

        while True:
            iocb = self.ioQueue.get()
            if not iocb:
                break
            if _debug: IOQController._debug("    - iocb: %r", iocb)

            # change the state
            iocb.ioState = ABORTED
            iocb.ioError = err

            # notify the client
            iocb.trigger()

        if (self.state != CTRL_IDLE):
            if _debug: IOQController._debug("    - busy after aborts")

    def request_io(self, iocb):
        """Called by a client to start processing a request."""
        if _debug: IOQController._debug("request_io %r", iocb)

        # bind the iocb to this controller
        iocb.ioController = self

        # if we're busy, queue it
        if (self.state != CTRL_IDLE):
            if _debug: IOQController._debug("    - busy, request queued")

            iocb.ioState = PENDING
            self.ioQueue.put(iocb)
            return

        try:
            # hopefully there won't be an error
            err = None

            # let derived class figure out how to process this
            self.process_io(iocb)
        except:
            # extract the error
            err = sys.exc_info()[1]

        # if there was an error, abort the request
        if err:
            self.abort_io(iocb, err)

    def process_io(self, iocb):
        """Figure out how to respond to this request.  This must be
        provided by the derived class."""
        raise NotImplementedError("IOController must implement process_io()")

    def active_io(self, iocb):
        """Called by a handler to notify the controller that a request is
        being processed."""
        if _debug: IOQController._debug("active_io %r", iocb)

        # base class work first, setting iocb state and timer data
        IOController.active_io(self, iocb)

        # change our state
        self.state = CTRL_ACTIVE

        # keep track of the iocb
        self.active_iocb = iocb

    def complete_io(self, iocb, msg):
        """Called by a handler to return data to the client."""
        if _debug: IOQController._debug("complete_io %r %r", iocb, msg)

        # check to see if it is completing the active one
        if iocb is not self.active_iocb:
            raise RuntimeError("not the current iocb")

        # normal completion
        IOController.complete_io(self, iocb, msg)

        # no longer an active iocb
        self.active_iocb = None

        # check to see if we should wait a bit
        if self.wait_time:
            # change our state
            self.state = CTRL_WAITING

            # schedule a call in the future
            task = FunctionTask(IOQController._wait_trigger, self)
            task.install_task(_time() + self.wait_time)

        else:
            # change our state
            self.state = CTRL_IDLE

            # look for more to do
            deferred(IOQController._trigger, self)

    def abort_io(self, iocb, err):
        """Called by a handler or a client to abort a transaction."""
        if _debug: IOQController._debug("abort_io %r %r", iocb, err)

        # normal abort
        IOController.abort_io(self, iocb, err)

        # check to see if it is completing the active one
        if iocb is not self.active_iocb:
            if _debug: IOQController._debug("    - not current iocb")
            return

        # no longer an active iocb
        self.active_iocb = None

        # change our state
        self.state = CTRL_IDLE

        # look for more to do
        deferred(IOQController._trigger, self)

    def _trigger(self):
        """Called to launch the next request in the queue."""
        if _debug: IOQController._debug("_trigger")

        # if we are busy, do nothing
        if self.state != CTRL_IDLE:
            if _debug: IOQController._debug("    - not idle")
            return

        # if there is nothing to do, return
        if not self.ioQueue.queue:
            if _debug: IOQController._debug("    - empty queue")
            return

        # get the next iocb
        iocb = self.ioQueue.get()

        try:
            # hopefully there won't be an error
            err = None

            # let derived class figure out how to process this
            self.process_io(iocb)
        except:
            # extract the error
            err = sys.exc_info()[1]

        # if there was an error, abort the request
        if err:
            self.abort_io(iocb, err)

        # if we're idle, call again
        if self.state == CTRL_IDLE:
            deferred(IOQController._trigger, self)

    def _wait_trigger(self):
        """Called to launch the next request in the queue."""
        if _debug: IOQController._debug("_wait_trigger")

        # make sure we are waiting
        if (self.state != CTRL_WAITING):
            raise RuntimeError("not waiting")

        # change our state
        self.state = CTRL_IDLE

        # look for more to do
        IOQController._trigger(self)

#
#   IOProxy
#

@bacpypes_debugging
class IOProxy:

    def __init__(self, controllerName, serverName=None, requestLimit=None):
        """Create an IO client.  It implements request_io like a controller, but
        passes requests on to a local controller if it happens to be in the 
        same process, or the IOProxyServer instance to forward on for processing."""
        if _debug: IOProxy._debug("__init__ %r serverName=%r, requestLimit=%r", controllerName, serverName, requestLimit)

        # save the server reference
        self.ioControllerRef = controllerName
        self.ioServerRef = serverName

        # set a limit on how many requests can be submitted
        self.ioRequestLimit = requestLimit
        self.ioPending = set()
        self.ioBlocked = deque()

        # bind to a local controller if possible
        if not serverName:
            self.ioBind = _local_controllers.get(controllerName, None)
            if self.ioBind:
                if _debug: IOProxy._debug("    - local bind successful")
            else:
                if _debug: IOProxy._debug("    - local bind deferred")
        else:
            self.ioBind = None
            if _debug: IOProxy._debug("    - bind deferred")

    def request_io(self, iocb, urgent=False):
        """Called by a client to start processing a request."""
        if _debug: IOProxy._debug("request_io %r urgent=%r", iocb, urgent)
        global _proxy_server

        # save the server and controller reference
        iocb.ioServerRef = self.ioServerRef
        iocb.ioControllerRef = self.ioControllerRef

        # check to see if it needs binding
        if not self.ioBind:
            # if the server is us, look for a local controller
            if not self.ioServerRef:
                self.ioBind = _local_controllers.get(self.ioControllerRef, None)
                if not self.ioBind:
                    iocb.abort("no local controller %s" % (self.ioControllerRef,))
                    return
                if _debug: IOProxy._debug("    - local bind successful")
            else:
                if not _proxy_server:
                    _proxy_server = IOProxyServer()

                self.ioBind = _proxy_server
                if _debug: IOProxy._debug("    - proxy bind successful: %r", self.ioBind)

        # if this isn't urgent and there is a limit, see if we've reached it
        if (not urgent) and self.ioRequestLimit:
            # call back when this is completed
            iocb.add_callback(self._proxy_trigger)

            # check for the limit
            if len(self.ioPending) < self.ioRequestLimit:
                if _debug: IOProxy._debug("    - cleared for launch")

                self.ioPending.add(iocb)
                self.ioBind.request_io(iocb)
            else:
                # save it for later
                if _debug: IOProxy._debug("    - save for later")

                self.ioBlocked.append(iocb)
        else:
            # just pass it along
            self.ioBind.request_io(iocb)

    def _proxy_trigger(self, iocb):
        """This has completed, remove it from the set of pending requests 
        and see if it's OK to start up the next one."""
        if _debug: IOProxy._debug("_proxy_trigger %r", iocb)

        if iocb not in self.ioPending:
            if _debug: IOProxy._warning("iocb not pending: %r", iocb)
        else:
            self.ioPending.remove(iocb)

            # check to send another one
            if (len(self.ioPending) < self.ioRequestLimit) and self.ioBlocked:
                nextio = self.ioBlocked.popleft()
                if _debug: IOProxy._debug("    - cleared for launch: %r", nextio)

                # this one is now pending
                self.ioPending.add(nextio)
                self.ioBind.request_io(nextio)

#
#   IOServer
#

PORT = 8002
SERVER_TIMEOUT = 60

@bacpypes_debugging
class IOServer(IOController, Client):

    def __init__(self, addr=('',PORT)):
        """Initialize the remote IO handler."""
        if _debug: IOServer._debug("__init__ %r", addr)
        IOController.__init__(self)

        # create a UDP director
        self.server = UDPDirector(addr)
        bind(self, self.server)

        # dictionary of IOCBs as a server
        self.remoteIOCB = {}

    def confirmation(self, pdu):
        if _debug: IOServer._debug('confirmation %r', pdu)

        addr = pdu.pduSource
        request = pdu.pduData

        try:
            # parse the request
            request = cPickle.loads(request)
            if _debug: _commlog.debug(">>> %s: S %s %r" % (_strftime(), str(addr), request))

            # pick the message
            if (request[0] == 0):
                self.new_iocb(addr, *request[1:])
            elif (request[0] == 1):
                self.complete_iocb(addr, *request[1:])
            elif (request[0] == 2):
                self.abort_iocb(addr, *request[1:])
        except:
            # extract the error
            err = sys.exc_info()[1]
            IOServer._exception("error %r processing %r from %r", err, request, addr)

    def callback(self, iocb):
        """Callback when an iocb is completed by a local controller and the
        result needs to be sent back to the client."""
        if _debug: IOServer._debug("callback %r", iocb)

        # make sure it's one of ours
        if not self.remoteIOCB.has_key(iocb):
            IOServer._warning("IOCB not owned by server: %r", iocb)
            return

        # get the client information
        clientID, clientAddr = self.remoteIOCB[iocb]

        # we're done with this
        del self.remoteIOCB[iocb]

        # build a response
        if iocb.ioState == COMPLETED:
            response = (1, clientID, iocb.ioResponse)
        elif iocb.ioState == ABORTED:
            response = (2, clientID, iocb.ioError)
        else:
            raise RuntimeError("IOCB invalid state")

        if _debug: _commlog.debug("<<< %s: S %s %r" % (_strftime(), clientAddr, response))

        response = cPickle.dumps( response, 1 )

        # send it to the client
        self.request(PDU(response, destination=clientAddr))

    def abort(self, err):
        """Called by a local application to abort all transactions."""
        if _debug: IOServer._debug("abort %r", err)

        for iocb in self.remoteIOCB.keys():
            self.abort_io(iocb, err)

    def abort_io(self, iocb, err):
        """Called by a local client or a local controlled to abort a transaction."""
        if _debug: IOServer._debug("abort_io %r %r", iocb, err)

        # if it completed, leave it alone
        if iocb.ioState == COMPLETED:
            pass

        # if it already aborted, leave it alone
        elif iocb.ioState == ABORTED:
            pass

        elif self.remoteIOCB.has_key(iocb):
            # get the client information
            clientID, clientAddr = self.remoteIOCB[iocb]

            # we're done with this
            del self.remoteIOCB[iocb]

            # build an abort response
            response = (2, clientID, err)
            if _debug: _commlog.debug("<<< %s: S %s %r" % (_strftime(), clientAddr, response))

            response = cPickle.dumps( response, 1 )

            # send it to the client
            self.socket.sendto( response, clientAddr )

        else:
            IOServer._error("no reference to aborting iocb: %r", iocb)

        # change the state
        iocb.ioState = ABORTED
        iocb.ioError = err

        # notify the client
        iocb.trigger()

    def new_iocb(self, clientAddr, iocbid, controllerName, args, kwargs):
        """Called when the server receives a new request."""
        if _debug: IOServer._debug("new_iocb %r %r %r %r %r", clientAddr, iocbid, controllerName, args, kwargs)

        # look for a controller
        controller = _local_controllers.get(controllerName, None)
        if not controller:
            # create a nice error message
            err = RuntimeError("no local controller '%s'" % (controllerName, ))

            # build an abort response
            response = (2, iocbid, err)
            if _debug: _commlog.debug("<<< %s: S %s %r" % (_strftime(), clientAddr, response))

            response = cPickle.dumps( response, 1 )

            # send it to the server
            self.request(PDU(response, destination=clientAddr))

        else:
            # create an IOCB
            iocb = IOCB(*args, **kwargs)
            if _debug: IOServer._debug("    - local IOCB %r bound to remote %r", iocb.ioID, iocbid)

            # save a reference to it
            self.remoteIOCB[iocb] = (iocbid, clientAddr)

            # make sure we're notified when it completes
            iocb.add_callback(self.callback)

            # pass it along
            controller.request_io(iocb)

    def abort_iocb(self, addr, iocbid, err):
        """Called when the client or server receives an abort request."""
        if _debug: IOServer._debug("abort_iocb %r %r %r", addr, iocbid, err)

        # see if this came from a client
        for iocb in self.remoteIOCB.keys():
            clientID, clientAddr = self.remoteIOCB[iocb]
            if (addr == clientAddr) and (clientID == iocbid):
                break
        else:
            IOServer._error("no reference to aborting iocb %r from %r", iocbid, addr)
            return
        if _debug: IOServer._debug("    - local IOCB %r bound to remote %r", iocb.ioID, iocbid)

        # we're done with this
        del self.remoteIOCB[iocb]

        # clear the callback, we already know
        iocb.ioCallback = []

        # tell the local controller about the abort
        iocb.abort(err)

#
#   IOProxyServer
#

SERVER_TIMEOUT = 60

@bacpypes_debugging
class IOProxyServer(IOController, Client):

    def __init__(self, addr=('', 0), name=None):
        """Initialize the remote IO handler."""
        if _debug: IOProxyServer._debug("__init__")
        IOController.__init__(self, name=name)

        # create a UDP director
        self.server = UDPDirector(addr)
        bind(self, self.server)
        if _debug: IOProxyServer._debug("    - bound to %r", self.server.socket.getsockname())

        # dictionary of IOCBs as a client
        self.localIOCB = {}

    def confirmation(self, pdu):
        if _debug: IOProxyServer._debug('confirmation %r', pdu)

        addr = pdu.pduSource
        request = pdu.pduData

        try:
            # parse the request
            request = cPickle.loads(request)
            if _debug: _commlog.debug(">>> %s: P %s %r" % (_strftime(), addr, request))

            # pick the message
            if (request[0] == 1):
                self.complete_iocb(addr, *request[1:])
            elif (request[0] == 2):
                self.abort_iocb(addr, *request[1:])
        except:
            # extract the error
            err = sys.exc_info()[1]
            IOProxyServer._exception("error %r processing %r from %r", err, request, addr)

    def process_io(self, iocb):
        """Package up the local IO request and send it to the server."""
        if _debug: IOProxyServer._debug("process_io %r", iocb)

        # save a reference in our dictionary
        self.localIOCB[iocb.ioID] = iocb

        # start a default timer if one hasn't already been set
        if not iocb.ioTimeout:
            iocb.set_timeout( SERVER_TIMEOUT, RuntimeError("no response from " + iocb.ioServerRef))

        # build a message
        request = (0, iocb.ioID, iocb.ioControllerRef, iocb.args, iocb.kwargs)
        if _debug: _commlog.debug("<<< %s: P %s %r" % (_strftime(), iocb.ioServerRef, request))

        request = cPickle.dumps( request, 1 )

        # send it to the server
        self.request(PDU(request, destination=(iocb.ioServerRef, PORT)))

    def abort(self, err):
        """Called by a local application to abort all transactions, local
        and remote."""
        if _debug: IOProxyServer._debug("abort %r", err)

        for iocb in self.localIOCB.values():
            self.abort_io(iocb, err)

    def abort_io(self, iocb, err):
        """Called by a local client or a local controlled to abort a transaction."""
        if _debug: IOProxyServer._debug("abort_io %r %r", iocb, err)

        # if it completed, leave it alone
        if iocb.ioState == COMPLETED:
            pass

        # if it already aborted, leave it alone
        elif iocb.ioState == ABORTED:
            pass

        elif self.localIOCB.has_key(iocb.ioID):
            # delete the dictionary reference
            del self.localIOCB[iocb.ioID]

            # build an abort request
            request = (2, iocb.ioID, err)
            if _debug: _commlog.debug("<<< %s: P %s %r" % (_strftime(), iocb.ioServerRef, request))

            request = cPickle.dumps( request, 1 )

            # send it to the server
            self.request(PDU(request, destination=(iocb.ioServerRef, PORT)))

        else:
            raise RuntimeError("no reference to aborting iocb: %r" % (iocb.ioID,))

        # change the state
        iocb.ioState = ABORTED
        iocb.ioError = err

        # notify the client
        iocb.trigger()

    def complete_iocb(self, serverAddr, iocbid, msg):
        """Called when the client receives a response to a request."""
        if _debug: IOProxyServer._debug("complete_iocb %r %r %r", serverAddr, iocbid, msg)

        # assume nothing
        iocb = None

        # make sure this is a local request
        if not self.localIOCB.has_key(iocbid):
            IOProxyServer._error("no reference to IOCB %r", iocbid)
            if _debug: IOProxyServer._debug("    - localIOCB: %r", self.localIOCB)
        else:
            # get the iocb
            iocb = self.localIOCB[iocbid]

            # delete the dictionary reference
            del self.localIOCB[iocbid]

        if iocb:
            # change the state
            iocb.ioState = COMPLETED
            iocb.ioResponse = msg

            # notify the client
            iocb.trigger()

    def abort_iocb(self, addr, iocbid, err):
        """Called when the client or server receives an abort request."""
        if _debug: IOProxyServer._debug("abort_iocb %r %r %r", addr, iocbid, err)

        if not self.localIOCB.has_key(iocbid):
            raise RuntimeError("no reference to aborting iocb: %r" % (iocbid,))

        # get the iocb
        iocb = self.localIOCB[iocbid]

        # delete the dictionary reference
        del self.localIOCB[iocbid]

        # change the state
        iocb.ioState = ABORTED
        iocb.ioError = err

        # notify the client
        iocb.trigger()

#
#   abort
#

@bacpypes_debugging
def abort(err):
    """Abort everything, everywhere."""
    if _debug: abort._debug("abort %r", err)

    # start with the server
    if IOServer._highlander:
        IOServer._highlander.abort(err)

    # now do everything local
    for controller in _local_controllers.values():
        controller.abort(err)

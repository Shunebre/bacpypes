#!/usr/bin/env python3

"""
Application Module
"""

import warnings

from .debugging import bacpypes_debugging, DebugContents, ModuleLogger

from .core import deferred
from .comm import ApplicationServiceElement, bind
from .iocb import IOController, SieveQueue

from .pdu import Address

from .primitivedata import ObjectIdentifier

from .capability import Collector
from .appservice import StateMachineAccessPoint, ApplicationServiceAccessPoint
from .netservice import NetworkServiceAccessPoint, NetworkServiceElement
from .bvllservice import BIPSimple, BIPForeign, AnnexJCodec, UDPMultiplexer

from .apdu import UnconfirmedRequestPDU, ConfirmedRequestPDU, \
    SimpleAckPDU, ComplexAckPDU, ErrorPDU, RejectPDU, AbortPDU, Error

from .errors import ExecutionError, UnrecognizedService, AbortException, RejectException

# for computing protocol services supported
from .apdu import confirmed_request_types, unconfirmed_request_types, \
    ConfirmedServiceChoice, UnconfirmedServiceChoice, \
    IAmRequest
from .basetypes import ServicesSupported

# basic services
from .service.device import WhoIsIAmServices
from .service.object import ReadWritePropertyServices

# some debugging
_debug = 0
_log = ModuleLogger(globals())

#
#   DeviceInfo
#

@bacpypes_debugging
class DeviceInfo(DebugContents):

    _debug_contents = (
        'deviceIdentifier',
        'address',
        'maxApduLengthAccepted',
        'segmentationSupported',
        'vendorID',
        'maxNpduLength',
        'maxSegmentsAccepted',
        )

    def __init__(self, device_identifier, address):
        # this information is from an IAmRequest
        self.deviceIdentifier = device_identifier
        self.address = address

        self.maxApduLengthAccepted = 1024               # maximum APDU device will accept
        self.segmentationSupported = 'noSegmentation'   # normally no segmentation
        self.maxSegmentsAccepted = None                 # None iff no segmentation
        self.vendorID = None                            # vendor identifier
        self.maxNpduLength = None           # maximum we can send in transit (see 19.4)

#
#   DeviceInfoCache
#

@bacpypes_debugging
class DeviceInfoCache:

    def __init__(self, device_info_class=DeviceInfo):
        if _debug: DeviceInfoCache._debug("__init__")

        # a little error checking
        if not issubclass(device_info_class, DeviceInfo):
            raise ValueError("not a DeviceInfo subclass: %r" % (device_info_class,))

        # empty cache
        self.cache = {}

        # class for new records
        self.device_info_class = device_info_class

    def has_device_info(self, key):
        """Return true iff cache has information about the device."""
        if _debug: DeviceInfoCache._debug("has_device_info %r", key)

        return key in self.cache

    def iam_device_info(self, apdu):
        """Create a device information record based on the contents of an
        IAmRequest and put it in the cache."""
        if _debug: DeviceInfoCache._debug("iam_device_info %r", apdu)

        # make sure the apdu is an I-Am
        if not isinstance(apdu, IAmRequest):
            raise ValueError("not an IAmRequest: %r" % (apdu,))

        # get the device instance
        device_instance = apdu.iAmDeviceIdentifier[1]

        # get the existing cache record if it exists
        device_info = self.cache.get(device_instance, None)

        # maybe there is a record for this address
        if not device_info:
            device_info = self.cache.get(apdu.pduSource, None)

        # make a new one using the class provided
        if not device_info:
            device_info = self.device_info_class(device_instance, apdu.pduSource)

        # jam in the correct values
        device_info.deviceIdentifier = device_instance
        device_info.address = apdu.pduSource
        device_info.maxApduLengthAccepted = apdu.maxAPDULengthAccepted
        device_info.segmentationSupported = apdu.segmentationSupported
        device_info.vendorID = apdu.vendorID

        # tell the cache this is an updated record
        self.update_device_info(device_info)

    def get_device_info(self, key):
        if _debug: DeviceInfoCache._debug("get_device_info %r", key)

        # get the info if it's there
        device_info = self.cache.get(key, None)
        if _debug: DeviceInfoCache._debug("    - device_info: %r", device_info)

        return device_info

    def update_device_info(self, device_info):
        """The application has updated one or more fields in the device
        information record and the cache needs to be updated to reflect the
        changes.  If this is a cached version of a persistent record then this
        is the opportunity to update the database."""
        if _debug: DeviceInfoCache._debug("update_device_info %r", device_info)

        # give this a reference count if it doesn't have one
        if not hasattr(device_info, '_ref_count'):
            device_info._ref_count = 0

        # get the current keys
        cache_id, cache_address = getattr(device_info, '_cache_keys', (None, None))

        if (cache_id is not None) and (device_info.deviceIdentifier != cache_id):
            if _debug: DeviceInfoCache._debug("    - device identifier updated")

            # remove the old reference, add the new one
            del self.cache[cache_id]
            self.cache[device_info.deviceIdentifier] = device_info

        if (cache_address is not None) and (device_info.address != cache_address):
            if _debug: DeviceInfoCache._debug("    - device address updated")

            # remove the old reference, add the new one
            del self.cache[cache_address]
            self.cache[device_info.address] = device_info

        # update the keys
        device_info._cache_keys = (device_info.deviceIdentifier, device_info.address)

    def acquire(self, key):
        """Return the known information about the device and mark the record
        as being used by a segmenation state machine."""
        if _debug: DeviceInfoCache._debug("acquire %r", key)

        if isinstance(key, int):
            device_info = self.cache.get(key, None)

        elif not isinstance(key, Address):
            raise TypeError("key must be integer or an address")

        elif key.addrType not in (Address.localStationAddr, Address.remoteStationAddr):
            raise TypeError("address must be a local or remote station")

        else:
            device_info = self.cache.get(key, None)

        if device_info:
            if _debug: DeviceInfoCache._debug("    - reference bump")
            device_info._ref_count += 1

        if _debug: DeviceInfoCache._debug("    - device_info: %r", device_info)

        return device_info

    def release(self, device_info):
        """This function is called by the segmentation state machine when it
        has finished with the device information."""
        if _debug: DeviceInfoCache._debug("release %r", device_info)

        # this information record might be used by more than one SSM
        if device_info._ref_count == 0:
            raise RuntimeError("reference count")

        # decrement the reference count
        device_info._ref_count -= 1

#
#   Application
#

@bacpypes_debugging
class Application(ApplicationServiceElement, Collector):

    _startup_disabled = False

    def __init__(self, localDevice=None, localAddress=None, deviceInfoCache=None, aseID=None):
        if _debug: Application._debug("__init__ %r %r deviceInfoCache=%r aseID=%r", localDevice, localAddress, deviceInfoCache, aseID)
        ApplicationServiceElement.__init__(self, aseID)

        # local objects by ID and name
        self.objectName = {}
        self.objectIdentifier = {}

        # keep track of the local device
        if localDevice:
            self.localDevice = localDevice

            # bind the device object to this application
            localDevice._app = self

            # local objects by ID and name
            self.objectName[localDevice.objectName] = localDevice
            self.objectIdentifier[localDevice.objectIdentifier] = localDevice

        # local address deprecated, but continue to use the old initializer
        if localAddress is not None:
            warnings.warn(
                "local address at the application layer deprecated",
                DeprecationWarning,
                )

            # allow the address to be cast to the correct type
            if isinstance(localAddress, Address):
                self.localAddress = localAddress
            else:
                self.localAddress = Address(localAddress)

        # use the provided cache or make a default one
        self.deviceInfoCache = deviceInfoCache or DeviceInfoCache()

        # controllers for managing confirmed requests as a client
        self.controllers = {}

        # now set up the rest of the capabilities
        Collector.__init__(self)

        # if starting up is enabled, find all the startup functions
        if not self._startup_disabled:
            for fn in self.capability_functions('startup'):
                if _debug: Application._debug("    - startup fn: %r" , fn)
                deferred(fn, self)

    def add_object(self, obj):
        """Add an object to the local collection."""
        if _debug: Application._debug("add_object %r", obj)

        # extract the object name and identifier
        object_name = obj.objectName
        if not object_name:
            raise RuntimeError("object name required")
        object_identifier = obj.objectIdentifier
        if not object_identifier:
            raise RuntimeError("object identifier required")

        # assuming the object identifier is well formed, check the instance number
        if (object_identifier[1] >= ObjectIdentifier.maximum_instance_number):
            raise RuntimeError("invalid object identifier")

        # make sure it hasn't already been defined
        if object_name in self.objectName:
            raise RuntimeError("already an object with name %r" % (object_name,))
        if object_identifier in self.objectIdentifier:
            raise RuntimeError("already an object with identifier %r" % (object_identifier,))

        # now put it in local dictionaries
        self.objectName[object_name] = obj
        self.objectIdentifier[object_identifier] = obj

        # append the new object's identifier to the local device's object list
        # if there is one and it has an object list property
        if self.localDevice and self.localDevice.objectList:
            self.localDevice.objectList.append(object_identifier)

        # let the object know which application stack it belongs to
        obj._app = self

    def delete_object(self, obj):
        """Add an object to the local collection."""
        if _debug: Application._debug("delete_object %r", obj)

        # extract the object name and identifier
        object_name = obj.objectName
        object_identifier = obj.objectIdentifier

        # delete it from the application
        del self.objectName[object_name]
        del self.objectIdentifier[object_identifier]

        # remove the object's identifier from the device's object list
        # if there is one and it has an object list property
        if self.localDevice and self.localDevice.objectList:
            indx = self.localDevice.objectList.index(object_identifier)
            del self.localDevice.objectList[indx]

        # make sure the object knows it's detached from an application
        obj._app = None

    def get_object_id(self, objid):
        """Return a local object or None."""
        return self.objectIdentifier.get(objid, None)

    def get_object_name(self, objname):
        """Return a local object or None."""
        return self.objectName.get(objname, None)

    def iter_objects(self):
        """Iterate over the objects."""
        return iter(self.objectIdentifier.values())

    def get_services_supported(self):
        """Return a ServicesSupported bit string based in introspection, look
        for helper methods that match confirmed and unconfirmed services."""
        if _debug: Application._debug("get_services_supported")

        services_supported = ServicesSupported()

        # look through the confirmed services
        for service_choice, service_request_class in confirmed_request_types.items():
            service_helper = "do_" + service_request_class.__name__
            if hasattr(self, service_helper):
                service_supported = ConfirmedServiceChoice._xlate_table[service_choice]
                services_supported[service_supported] = 1

        # look through the unconfirmed services
        for service_choice, service_request_class in unconfirmed_request_types.items():
            service_helper = "do_" + service_request_class.__name__
            if hasattr(self, service_helper):
                service_supported = UnconfirmedServiceChoice._xlate_table[service_choice]
                services_supported[service_supported] = 1

        # return the bit list
        return services_supported

    #-----

    def request(self, apdu):
        if _debug: Application._debug("request %r", apdu)

        # double check the input is the right kind of APDU
        if not isinstance(apdu, (UnconfirmedRequestPDU, ConfirmedRequestPDU)):
            raise TypeError("APDU expected")

        # continue
        super(Application, self).request(apdu)

    def indication(self, apdu):
        if _debug: Application._debug("indication %r", apdu)

        # get a helper function
        helperName = "do_" + apdu.__class__.__name__
        helperFn = getattr(self, helperName, None)
        if _debug: Application._debug("    - helperFn: %r", helperFn)

        # send back a reject for unrecognized services
        if not helperFn:
            if isinstance(apdu, ConfirmedRequestPDU):
                raise UnrecognizedService("no function %s" % (helperName,))
            return

        # pass the apdu on to the helper function
        try:
            helperFn(apdu)
        except RejectException as err:
            if _debug: Application._debug("    - reject exception: %r", err)
            raise
        except AbortException as err:
            if _debug: Application._debug("    - abort exception: %r", err)
            raise
        except ExecutionError as err:
            if _debug: Application._debug("    - execution error: %r", err)

            # send back an error
            if isinstance(apdu, ConfirmedRequestPDU):
                resp = Error(errorClass=err.errorClass, errorCode=err.errorCode, context=apdu)
                self.response(resp)

        except Exception as err:
            Application._exception("exception: %r", err)

            # send back an error
            if isinstance(apdu, ConfirmedRequestPDU):
                resp = Error(errorClass='device', errorCode='operationalProblem', context=apdu)
                self.response(resp)

#
#   ApplicationIOController
#

@bacpypes_debugging
class ApplicationIOController(IOController, Application):

    def __init__(self, *args, **kwargs):
        if _debug: ApplicationIOController._debug("__init__")
        IOController.__init__(self)
        Application.__init__(self, *args, **kwargs)

        # queues for each address
        self.queue_by_address = {}

    def process_io(self, iocb):
        if _debug: ApplicationIOController._debug("process_io %r", iocb)

        # get the destination address from the pdu
        destination_address = iocb.args[0].pduDestination
        if _debug: ApplicationIOController._debug("    - destination_address: %r", destination_address)

        # look up the queue
        queue = self.queue_by_address.get(destination_address, None)
        if not queue:
            queue = SieveQueue(self._app_request, destination_address)
            self.queue_by_address[destination_address] = queue
        if _debug: ApplicationIOController._debug("    - queue: %r", queue)

        # ask the queue to process the request
        queue.request_io(iocb)

    def _app_complete(self, address, apdu):
        if _debug: ApplicationIOController._debug("_app_complete %r %r", address, apdu)

        # look up the queue
        queue = self.queue_by_address.get(address, None)
        if not queue:
            ApplicationIOController._debug("no queue for %r" % (address,))
            return
        if _debug: ApplicationIOController._debug("    - queue: %r", queue)

        # make sure it has an active iocb
        if not queue.active_iocb:
            ApplicationIOController._debug("no active request for %r" % (address,))
            return

        # this request is complete
        if isinstance(apdu, (None.__class__, SimpleAckPDU, ComplexAckPDU)):
            queue.complete_io(queue.active_iocb, apdu)
        elif isinstance(apdu, (ErrorPDU, RejectPDU, AbortPDU)):
            queue.abort_io(queue.active_iocb, apdu)
        else:
            raise RuntimeError("unrecognized APDU type")
        if _debug: Application._debug("    - controller finished")

        # if the queue is empty and idle, forget about the controller
        if not queue.ioQueue.queue and not queue.active_iocb:
            if _debug: ApplicationIOController._debug("    - queue is empty")
            del self.queue_by_address[address]

    def _app_request(self, apdu):
        if _debug: ApplicationIOController._debug("_app_request %r", apdu)

        # send it downstream, bypass the guard
        super(ApplicationIOController, self).request(apdu)

        # if this was an unconfirmed request, it's complete, no message
        if isinstance(apdu, UnconfirmedRequestPDU):
            self._app_complete(apdu.pduDestination, None)

    def request(self, apdu):
        if _debug: ApplicationIOController._debug("request %r", apdu)

        # if this is not unconfirmed request, tell the application to use
        # the IOCB interface
        if not isinstance(apdu, UnconfirmedRequestPDU):
            raise RuntimeError("use IOCB for confirmed requests")

        # send it downstream
        super(ApplicationIOController, self).request(apdu)

    def confirmation(self, apdu):
        if _debug: ApplicationIOController._debug("confirmation %r", apdu)

        # this is an ack, error, reject or abort
        self._app_complete(apdu.pduSource, apdu)

#
#   BIPSimpleApplication
#

@bacpypes_debugging
class BIPSimpleApplication(ApplicationIOController, WhoIsIAmServices, ReadWritePropertyServices):

    def __init__(self, localDevice, localAddress, deviceInfoCache=None, aseID=None):
        if _debug: BIPSimpleApplication._debug("__init__ %r %r deviceInfoCache=%r aseID=%r", localDevice, localAddress, deviceInfoCache, aseID)
        ApplicationIOController.__init__(self, localDevice, localAddress, deviceInfoCache, aseID=aseID)

        # local address might be useful for subclasses
        if isinstance(localAddress, Address):
            self.localAddress = localAddress
        else:
            self.localAddress = Address(localAddress)

        # include a application decoder
        self.asap = ApplicationServiceAccessPoint()

        # pass the device object to the state machine access point so it
        # can know if it should support segmentation
        self.smap = StateMachineAccessPoint(localDevice)

        # the segmentation state machines need access to the same device
        # information cache as the application
        self.smap.deviceInfoCache = self.deviceInfoCache

        # a network service access point will be needed
        self.nsap = NetworkServiceAccessPoint()

        # give the NSAP a generic network layer service element
        self.nse = NetworkServiceElement()
        bind(self.nse, self.nsap)

        # bind the top layers
        bind(self, self.asap, self.smap, self.nsap)

        # create a generic BIP stack, bound to the Annex J server
        # on the UDP multiplexer
        self.bip = BIPSimple()
        self.annexj = AnnexJCodec()
        self.mux = UDPMultiplexer(self.localAddress)

        # bind the bottom layers
        bind(self.bip, self.annexj, self.mux.annexJ)

        # bind the BIP stack to the network, no network number
        self.nsap.bind(self.bip, address=self.localAddress)

    def close_socket(self):
        if _debug: BIPSimpleApplication._debug("close_socket")

        # pass to the multiplexer, then down to the sockets
        self.mux.close_socket()

#
#   BIPForeignApplication
#

@bacpypes_debugging
class BIPForeignApplication(ApplicationIOController, WhoIsIAmServices, ReadWritePropertyServices):

    def __init__(self, localDevice, localAddress, bbmdAddress, bbmdTTL, deviceInfoCache=None, aseID=None):
        if _debug:
            BIPForeignApplication._debug(
                "__init__ %r %r %r %r deviceInfoCache=%r aseID=%r",
                localDevice, localAddress, bbmdAddress, bbmdTTL,
                deviceInfoCache, aseID,
                )
        ApplicationIOController.__init__(self, localDevice, localAddress, deviceInfoCache, aseID=aseID)

        # local address might be useful for subclasses
        if isinstance(localAddress, Address):
            self.localAddress = localAddress
        else:
            self.localAddress = Address(localAddress)

        # include a application decoder
        self.asap = ApplicationServiceAccessPoint()

        # pass the device object to the state machine access point so it
        # can know if it should support segmentation
        self.smap = StateMachineAccessPoint(localDevice)

        # the segmentation state machines need access to the same device
        # information cache as the application
        self.smap.deviceInfoCache = self.deviceInfoCache

        # a network service access point will be needed
        self.nsap = NetworkServiceAccessPoint()

        # give the NSAP a generic network layer service element
        self.nse = NetworkServiceElement()
        bind(self.nse, self.nsap)

        # bind the top layers
        bind(self, self.asap, self.smap, self.nsap)

        # create a generic BIP stack, bound to the Annex J server
        # on the UDP multiplexer
        self.bip = BIPForeign(bbmdAddress, bbmdTTL)
        self.annexj = AnnexJCodec()
        self.mux = UDPMultiplexer(self.localAddress, noBroadcast=True)

        # bind the bottom layers
        bind(self.bip, self.annexj, self.mux.annexJ)

        # bind the NSAP to the stack, no network number
        self.nsap.bind(self.bip, address=self.localAddress)

    def close_socket(self):
        if _debug: BIPForeignApplication._debug("close_socket")

        # pass to the multiplexer, then down to the sockets
        self.mux.close_socket()

#
#   BIPNetworkApplication
#

@bacpypes_debugging
class BIPNetworkApplication(NetworkServiceElement):

    def __init__(self, localAddress, bbmdAddress=None, bbmdTTL=None, eID=None):
        if _debug: BIPNetworkApplication._debug("__init__ %r eID=%r", localAddress, eID)
        NetworkServiceElement.__init__(self, eID)

        # allow the address to be cast to the correct type
        if isinstance(localAddress, Address):
            self.localAddress = localAddress
        else:
            self.localAddress = Address(localAddress)

        # a network service access point will be needed
        self.nsap = NetworkServiceAccessPoint()

        # give the NSAP a generic network layer service element
        bind(self, self.nsap)

        # create a generic BIP stack, bound to the Annex J server
        # on the UDP multiplexer
        if (not bbmdAddress) and (not bbmdTTL):
            self.bip = BIPSimple()
        else:
            self.bip = BIPForeign(bbmdAddress, bbmdTTL)
        self.annexj = AnnexJCodec()
        self.mux = UDPMultiplexer(self.localAddress, noBroadcast=False)

        # bind the bottom layers
        bind(self.bip, self.annexj, self.mux.annexJ)

        # bind the NSAP to the stack, no network number
        self.nsap.bind(self.bip, address=self.localAddress)


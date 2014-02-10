"""A PyZMQ based RPC service.

Authors:

* Brian Granger
* Alexander Glyzov
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012. Brian Granger, Min Ragan-Kelley  
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file COPYING.BSD, distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import sys
import traceback

from abc import ABCMeta, abstractmethod

import gevent
import zmq

from zmq                     import green
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop    import IOLoop
from zmq.utils               import jsonapi

from .serializer import PickleSerializer


def rpc_method(f):
    """A decorator for use in declaring a method as an rpc method.

    Use as follows::

        @rpc_method
        def echo(self, s):
            return s
    """
    f.is_rpc_method = True
    return f


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class RPCBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, serializer=None):
        """Base class for RPC service and proxy.

        Parameters
        ==========
        serializer : Serializer
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        self.socket = None
        self._serializer = serializer if serializer is not None else PickleSerializer()
        self.reset()

    @abstractmethod
    def _create_socket(self):
        pass

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def reset(self):
        """Reset the socket/stream."""
        if isinstance(self.socket, (zmq.Socket, ZMQStream)):
            self.socket.close()
        self._create_socket()
        self.urls = []

    def bind(self, url):
        """Bind the service to a url of the form proto://ip:port."""
        self.socket.bind(url)
        self.urls.append(url)

    def bind_ports(self, ip, ports):
        """Try to bind a socket to the first available tcp port.

        The ports argument can either be an integer valued port
        or a list of ports to try. This attempts the following logic:

        * If ports==0, we bind to a random port.
        * If ports > 0, we bind to port.
        * If ports is a list, we bind to the first free port in that list.

        In all cases we save the eventual url that we bind to.

        This raises zmq.ZMQBindError if no free port can be found.
        """
        if isinstance(ports, int):
            ports = [ports]
        for p in ports:
            try:
                if p==0:
                    port = self.socket.bind_to_random_port("tcp://%s" % ip)
                else:
                    self.socket.bind("tcp://%s:%i" % (ip, p))
                    port = p
            except zmq.ZMQError:
                # bind raises this if the port is not free
                continue
            except zmq.ZMQBindError:
                # bind_to_random_port raises this if no port could be found
                continue
            else:
                break
        else:
            raise zmq.ZMQBindError('Could not find an available port')
        url = 'tcp://%s:%i' % (ip, port)
        self.urls.append(url)
        return port

    def connect(self, url):
        """Connect the service to a url of the form proto://ip:port."""
        self.socket.connect(url)
        self.urls.append(url)

class RPCServiceBase(RPCBase):

    def _build_reply(self, status, data):
        """Build a reply message for status and data.

        Parameters
        ----------
        status : bytes
            Either b'SUCCESS' or b'FAILURE'.
        data : list of bytes
            A list of data frame to be appended to the message.
        """
        reply = []
        reply.extend(self.idents)
        reply.extend([b'|', self.msg_id, status])
        reply.extend(data)
        return reply

    def _handle_request(self, msg_list):
        """Handle an incoming request.

        The request is received as a multipart message:

        [<idents>, b'|', msg_id, method, <sequence of serialized args/kwargs>]

        The reply depends on if the call was successful or not:

        [<idents>, b'|', msg_id, 'SUCCESS', <sequece of serialized result>]
        [<idents>, b'|', msg_id, 'FAILURE', <JSON dict of ename, evalue, traceback>]

        Here the (ename, evalue, traceback) are utf-8 encoded unicode.
        """
        i = msg_list.index(b'|')
        self.idents = msg_list[0:i]
        self.msg_id = msg_list[i+1]
        method = msg_list[i+2]
        data = msg_list[i+3:]
        args, kwargs = self._serializer.deserialize_args_kwargs(data)

        # Find and call the actual handler for message.
        try:
            handler = getattr(self, method, None)
            if handler is None or not getattr(handler, 'is_rpc_method', False):
                raise NotImplementedError("Unknown RPC method %r" % method)
            result = handler(*args, **kwargs)
        except Exception:
            self._send_error()
        else:
            try:
                data_list = self._serializer.serialize_result(result)
            except Exception:
                self._send_error()
            else:
                reply = self._build_reply(b'SUCCESS', data_list)
                self.socket.send_multipart(reply)

        self.idents = None
        self.msg_id = None

    def _send_error(self):
        """Send an error reply."""
        etype, evalue, tb = sys.exc_info()
        error_dict = {
            'ename' : str(etype.__name__),
            'evalue' : str(evalue),
            'traceback' : traceback.format_exc(tb)
        }
        data_list = [jsonapi.dumps(error_dict)]
        reply = self._build_reply(b'FAILURE', data_list)
        self.socket.send_multipart(reply)

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    @abstractmethod
    def start(self):
        """Start the service"""
        pass

    @abstractmethod
    def serve(self):
        """Serve RPC requests"""
        pass

class TornadoRPCService(RPCServiceBase):
    """ An asynchronous RPC service that takes requests over a ROUTER socket.
        Using Tornado compatible IOLoop and ZMQStream from pyzmq.
    """

    def __init__(self, context=None, ioloop=None, **kwargs):
        """
        Parameters
        ==========
        ioloop : IOLoop
            An existing IOLoop instance, if not passed, zmq.IOLoop.instance()
            will be used.
        context : Context
            An existing Context instance, if not passed, zmq.Context.instance()
            will be used.
        serializer : Serializer
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        assert context is None or isinstance(context, zmq.Context)
        self.context = context if context is not None else zmq.Context.instance()
        self.ioloop  = IOLoop.instance() if ioloop is None else ioloop
        super(TornadoRPCService, self).__init__(**kwargs)

    def _create_socket(self):
        socket = self.context.socket(zmq.ROUTER)
        self.socket = ZMQStream(socket, self.ioloop)
        # register IOLoop callback
        self.socket.on_recv(self._handle_request)

    def start(self):
        """ Start the RPC service (non-blocking) """
        pass  # no-op since IOLoop handler is already registered

    def serve(self):
        """ Serve RPC requests (blocking) """
        return self.ioloop.start()

class GeventRPCService(RPCServiceBase):
    """ An asynchronous RPC service that takes requests over a ROUTER socket.
        Using Gevent compatibility layer from pyzmq (zmq.green).
    """

    def __init__(self, context=None, **kwargs):
        """
        Parameters
        ==========
        context : Context
            An existing Context instance, if not passed, green.Context.instance()
            will be used.
        serializer : Serializer
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        assert context is None or isinstance(context, green.Context)
        self.context  = context if context is not None else green.Context.instance()
        self.greenlet = None
        super(GeventRPCService, self).__init__(**kwargs)

    def _create_socket(self):
        self.socket = self.context.socket(zmq.ROUTER)

    def start(self):
        """ Start the RPC service (non-blocking).

            Spawns a receive-reply greenlet that serves this socket.
            Returns spawned greenlet instance.
        """
        assert self.urls, 'not bound?'
        assert self.greenlet is None, 'already started'

        def receive_reply():
            while True:
                try:
                    request = self.socket.recv_multipart()
                except Exception, e:
                    print e
                    break
                gevent.spawn(self._handle_request, request)
            self.greenlet = None  # cleanup

        self.greenlet = gevent.spawn(receive_reply)
        return self.greenlet

    def serve(self, greenlets=[]):
        """ Serve RPC requests (blocking)

            Waits for specified greenlets or for this greenlet
        """
        if greenlets:
            return gevent.joinall(greenlets)
        else:
            if self.greenlet is None:
                self.start()
            return self.greenlet.join()


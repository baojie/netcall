# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=indent

"""A PyZMQ based RPC service.

Authors:

* Brian Granger
* Alexander Glyzov
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file COPYING.BSD, distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import sys
import traceback

from abc import abstractmethod

import zmq

from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop    import IOLoop
from zmq.utils               import jsonapi

from .base import RPCBase


#-----------------------------------------------------------------------------
# RPC utilities
#-----------------------------------------------------------------------------

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
        super(TornadoRPCService, self)._create_socket()
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


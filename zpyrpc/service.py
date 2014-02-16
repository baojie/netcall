# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
An RPC service using ZeroMQ as a transport.

Authors:

* Brian Granger
* Alexander Glyzov
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import sys
import traceback

from functools import partial
from abc       import abstractmethod

import zmq

from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop    import IOLoop
from zmq.utils               import jsonapi

from .base import RPCBase


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class RPCServiceBase(RPCBase):  #{

    _RESERVED = ['registser','proc','task','start','stop','serve']

    def __init__(self, *args, **kwargs):  #{
        super(RPCServiceBase, self).__init__(*args, **kwargs)
        self.procedures = {}  # {<name> : <callable>}

        # register extra class methods as service procedures
        for name in dir(self):
            if name.startswith('_') or name in self._RESERVED:
                continue
            try:    proc = getattr(self, name)
            except: continue
            if callable(proc):
                self.procedures[name] = proc
    #}
    def _build_reply(self, status, data):  #{
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
    #}
    def _send_error(self):  #{
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
    #}
    def _handle_request(self, msg_list):  #{
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
        name = msg_list[i+2]
        data = msg_list[i+3:]
        args, kwargs = self._serializer.deserialize_args_kwargs(data)

        # Find and call the actual handler for message.
        try:
            proc = self.procedures.get(name, None)
            if proc is None:
                raise NotImplementedError("Unregistered procedure %r" % name)
            result = proc(*args, **kwargs)
            data_list = self._serializer.serialize_result(result)
            reply = self._build_reply(b'SUCCESS', data_list)
            self.socket.send_multipart(reply)
        except Exception:
            self._send_error()

        self.idents = None
        self.msg_id = None
    #}

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def register(self, func=None, name=None):  #{
        """ A decorator to register a callable as a service task.

            Examples:

            service = TornadoRPCService()

            @service.task
            def echo(s):
                return s

            @service.proc(name='work')
            def do_nothing():
                pass

            service.register(lambda: None, name='dummy')
        """
        if func is None:
            if name is None:
                raise ValueError("at least one argument is required")
            return partial(self.register, name=name)
        else:
            if not callable(func):
                raise ValueError("func argument should be callable")
            if name is None:
                name = func.__name__
            self.procedures[name] = func

        return func
    #}

    task = register  # alias
    proc = register  # alias

    @abstractmethod
    def start(self):  #{
        """ Start the service (non-blocking) """
        pass
    #}

    @abstractmethod
    def stop(self):  #{
        """ Stop the service (non-blocking) """
        pass
    #}

    @abstractmethod
    def serve(self):  #{
        """ Serve RPC requests (blocking) """
        pass
    #}
#}

class TornadoRPCService(RPCServiceBase):  #{
    """ An asynchronous RPC service that takes requests over a ROUTER socket.
        Using Tornado compatible IOLoop and ZMQStream from PyZMQ.
    """

    def __init__(self, context=None, ioloop=None, **kwargs):  #{
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
        self._is_started = False
        super(TornadoRPCService, self).__init__(**kwargs)
    #}
    def _create_socket(self):  #{
        super(TornadoRPCService, self)._create_socket()
        socket = self.context.socket(zmq.ROUTER)
        self.socket = ZMQStream(socket, self.ioloop)
    #}
    def start(self):  #{
        """ Start the RPC service (non-blocking) """
        assert self._is_started == False, "already started"
        # register IOLoop callback
        self._is_started = True
        self.socket.on_recv(self._handle_request)
    #}
    def stop(self):  #{
        """ Stop the RPC service (non-blocking) """
        # register IOLoop callback
        self.socket.stop_on_recv()
        self._is_started = False
    #}
    def serve(self):  #{
        """ Serve RPC requests (blocking) """
        if not self._is_started:
            self.start()
        return self.ioloop.start()
    #}
#}

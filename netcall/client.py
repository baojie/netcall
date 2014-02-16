# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=indent

"""
Client/proxy classes to talk to a NetCall service.

Authors:

* Brian Granger
* Alexander Glyzov
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file COPYING.BSD, distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import logging
import sys
import traceback
import uuid

import zmq

from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop    import IOLoop, DelayedCallback
from zmq.utils               import jsonapi

from .base import RPCBase

#-----------------------------------------------------------------------------
# RPC Service Proxy
#-----------------------------------------------------------------------------

class RPCClientBase(RPCBase):
    """A service proxy to for talking to an RPCService."""

    def _create_socket(self):
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, bytes(uuid.uuid4()))

    def _build_request(self, method, args, kwargs):
        msg_id = bytes(uuid.uuid4())
        method = bytes(method)
        msg_list = [b'|', msg_id, method]
        data_list = self._serializer.serialize_args_kwargs(args, kwargs)
        msg_list.extend(data_list)
        return msg_id, msg_list

    def __getattr__(self, name):
        return RemoteMethod(self, name)


class SyncRPCClient(RPCClientBase):
    """A synchronous service proxy whose requests will block."""

    def __init__(self, context=None, **kwargs):
        """
        Parameters
        ==========
        context : Context
            An existing Context instance, if not passed, zmq.Context.instance()
            will be used.
        serializer : Serializer
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        assert context is None or isinstance(context, zmq.Context)
        self.context = context if context is not None else zmq.Context.instance()
        super(SyncRPCClient, self).__init__(**kwargs)

    def call(self, method, *args, **kwargs):
        """Call the remote method with *args and **kwargs.

        Parameters
        ----------
        method : str
            The name of the remote method to call.
        args : tuple
            The tuple of arguments to pass as `*args` to the RPC method.
        kwargs : dict
            The dict of arguments to pass as `**kwargs` to the RPC method.

        Returns
        -------
        result : object
            If the call succeeds, the result of the call will be returned.
            If the call fails, `RemoteRPCError` will be raised.
        """
        if not self._ready:
            raise RuntimeError('bind or connect must be called first')

        msg_id, msg_list = self._build_request(method, args, kwargs)

        self.socket.send_multipart(msg_list)
        msg_list = self.socket.recv_multipart()

        if not msg_list[0] == b'|':
            raise RPCError('Unexpected reply message format in RPCClient._handle_reply')

        #msg_id = msg_list[1]
        status = msg_list[2]

        if status == b'SUCCESS':
            result = self._serializer.deserialize_result(msg_list[3:])
            return result
        elif status == b'FAILURE':
            error_dict = jsonapi.loads(msg_list[3])
            raise RemoteRPCError(error_dict['ename'], error_dict['evalue'], error_dict['traceback'])


class TornadoRPCClient(RPCClientBase):
    """An asynchronous service proxy (based on Tornado IOLoop)"""

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
        self.context    = context if context is not None else zmq.Context.instance()
        self.ioloop     = IOLoop.instance() if ioloop is None else ioloop
        self._callbacks = {}
        super(TornadoRPCClient, self).__init__(**kwargs)

    def _create_socket(self):
        super(TornadoRPCClient, self)._create_socket()
        self.socket = ZMQStream(self.socket, self.ioloop)
        self.socket.on_recv(self._handle_reply)

    def _handle_reply(self, msg_list):
        # msg_list[0] == b'|'
        if not msg_list[0] == b'|':
            logging.error('Unexpected reply message format in TornadoRPCClient._handle_reply')
            return
        msg_id = msg_list[1]
        status = msg_list[2]
        cb_eb_dc = self._callbacks.pop(msg_id, None) # (cb, eb) tuple
        if cb_eb_dc is not None:
            cb, eb, dc = cb_eb_dc
            # Stop the timeout if there was one.
            if dc is not None:
                dc.stop()
            if status == b'SUCCESS' and cb is not None:
                result = self._serializer.deserialize_result(msg_list[3:])
                try:
                    cb(result)
                except:
                    logging.error('Unexpected callback error', exc_info=True)
            elif status == b'FAILURE' and eb is not None:
                error_dict = jsonapi.loads(msg_list[3])
                try:
                    eb(error_dict['ename'], error_dict['evalue'], error_dict['traceback'])
                except:
                    logging.error('Unexpected errback error', exc_info=True)

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def __getattr__(self, name):
        return AsyncRemoteMethod(self, name)

    def call(self, method, callback, errback, timeout, *args, **kwargs):
        """Call the remote method with *args and **kwargs.

        Parameters
        ----------
        method : str
            The name of the remote method to call.
        callback : callable
            The callable to call upon success or None. The result of the RPC
            call is passed as the single argument to the callback:
            `callback(result)`.
        errback : callable
            The callable to call upon a remote exception or None, The
            signature of this method is `errback(ename, evalue, tb)` where
            the arguments are passed as strings.
        timeout : int
            The number of milliseconds to wait before aborting the request.
            When a request is aborted, the errback will be called with an
            RPCTimeoutError. Set to 0 or a negative number to use an infinite
            timeout.
        args : tuple
            The tuple of arguments to pass as `*args` to the RPC method.
        kwargs : dict
            The dict of arguments to pass as `**kwargs` to the RPC method.
        """
        if not isinstance(timeout, int):
            raise TypeError("int expected, got %r" % timeout)
        if not (callback is None or callable(callback)):
            raise TypeError("callable or None expected, got %r" % callback)
        if not (errback is None or callable(errback)):
            raise TypeError("callable or None expected, got %r" % errback)

        msg_id, msg_list = self._build_request(method, args, kwargs)
        self.socket.send_multipart(msg_list)

        # The following logic assumes that the reply won't come back too
        # quickly, otherwise the callbacks won't be in place in time. It should
        # be fine as this code should run very fast. This approach improves
        # latency we send the request ASAP.
        def _abort_request():
            cb_eb_dc = self._callbacks.pop(msg_id, None)
            if cb_eb_dc is not None:
                eb = cb_eb_dc[1]
                if eb is not None:
                    try:
                        raise RPCTimeoutError()
                    except:
                        etype, evalue, tb = sys.exc_info()
                        eb(etype.__name__, evalue, traceback.format_exc(tb))
        if timeout > 0:
            dc = DelayedCallback(_abort_request, timeout, self.ioloop)
            dc.start()
        else:
            dc = None

        self._callbacks[msg_id] = (callback, errback, dc)


class RemoteMethodBase(object):
    """A remote method class to enable a nicer call syntax."""

    def __init__(self, proxy, method):
        self.proxy = proxy
        self.method = method


class AsyncRemoteMethod(RemoteMethodBase):

    def __call__(self, callback, *args, **kwargs):
        return self.proxy.call(self.method, callback, *args, **kwargs)


class RemoteMethod(RemoteMethodBase):

    def __call__(self, *args, **kwargs):
        return self.proxy.call(self.method, *args, **kwargs)

class RPCError(Exception):
    pass


class RemoteRPCError(RPCError):
    """Error raised elsewhere"""
    ename = None
    evalue = None
    traceback = None

    def __init__(self, ename, evalue, tb):
        self.ename = ename
        self.evalue = evalue
        self.traceback = tb
        self.args = (ename, evalue)

    def __repr__(self):
        return "<RemoteError:%s(%s)>" % (self.ename, self.evalue)

    def __str__(self):
        sig = "%s(%s)" % (self.ename, self.evalue)
        if self.traceback:
            return self.traceback
        else:
            return sig

class RPCTimeoutError(RPCError):
    pass


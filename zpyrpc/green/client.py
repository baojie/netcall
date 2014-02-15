# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
Gevent version of the RPC client

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

from zmq       import green
from zmq.utils import jsonapi

from gevent       import spawn
from gevent.event import Event, AsyncResult

from ..client import RPCClientBase, RPCError, RemoteRPCError

#-----------------------------------------------------------------------------
# RPC Service Proxy
#-----------------------------------------------------------------------------

class GeventRPCClient(RPCClientBase):
    """ An asynchronous service proxy whose requests will not block.
        Uses the Gevent compatibility layer of pyzmq (zmq.green).
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
        self.context   = context if context is not None else green.Context.instance()
        self._ready_ev = Event()
        self._exit_ev  = Event()
        self.greenlet  = spawn(self._reader)
        self._results  = {}    # {<msg-id> : <gevent.AsyncResult>}
        super(GeventRPCClient, self).__init__(**kwargs)  # base class

    def _create_socket(self):
        super(GeventRPCClient, self)._create_socket()

    def bind(self, url):
        result = super(GeventRPCClient, self).bind(url)
        self._ready_ev.set()  # wake up _reader
        return result

    def bind_ports(self, *args, **kwargs):
        result = super(GeventRPCClient, self).bind_ports(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result

    def connect(self, url):
        result = super(GeventRPCClient, self).connect(url)
        self._ready_ev.set()  # wake up _reader
        return result

    def _reader(self):
        """ Reader greenlet

            Waits for a socket to become ready (._ready_ev), then reads incoming replies and
            fills matching async results thus passing control to waiting greenlets (see .call)
        """
        ready_ev = self._ready_ev
        exit_ev  = self._exit_ev
        socket   = self.socket
        results  = self._results

        while True:
            ready_ev.wait()  # block until socket is bound/connected
            self._ready_ev.clear()

            if exit_ev.is_set():
                break  # a way to end the reader greenlet (see .shutdown())

            while self._ready:
                try:
                    msg_list = socket.recv_multipart()
                except Exception, e:
                    # the socket must have been closed
                    print e
                    break

                if not msg_list[0] == b'|':
                    err_msg = 'Unexpected reply message format in GeventRPCClient._reader'
                    print err_msg
                    raise RPCError(err_msg)

                # look for matching async result
                msg_id = msg_list[1]
                if msg_id in results:
                    results[msg_id].set(msg_list)
                    del results[msg_id]

        print 'EXIT'

    def shutdown(self):
        """Close the socket and signal the reader greenlet to exit"""
        self._ready = False
        self._exit_ev.set()
        self._ready_ev.set()
        self.socket.close()
        self._ready_ev.clear()

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
        result = AsyncResult()
        self._results[msg_id] = result

        self.socket.send_multipart(msg_list)
        msg_list = result.get()  # block waiting for a reply passed by ._reader

        status = msg_list[2]

        if status == b'SUCCESS':
            result = self._serializer.deserialize_result(msg_list[3:])
            return result
        elif status == b'FAILURE':
            error_dict = jsonapi.loads(msg_list[3])
            raise RemoteRPCError(error_dict['ename'], error_dict['evalue'], error_dict['traceback'])


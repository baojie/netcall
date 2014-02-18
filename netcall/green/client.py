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

from logging import getLogger

from zmq import green

from gevent       import spawn, spawn_later
from gevent.event import Event, AsyncResult

from ..client import RPCClientBase, RPCTimeoutError


logger = getLogger("netcall")

#-----------------------------------------------------------------------------
# RPC Service Proxy
#-----------------------------------------------------------------------------

class GeventRPCClient(RPCClientBase):
    """ An asynchronous service proxy whose requests will not block.
        Uses the Gevent compatibility layer of pyzmq (zmq.green).
    """

    def __init__(self, context=None, **kwargs):  #{
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
    #}
    def _create_socket(self):  #{
        super(GeventRPCClient, self)._create_socket()
    #}
    def bind(self, url):  #{
        result = super(GeventRPCClient, self).bind(url)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def bind_ports(self, *args, **kwargs):  #{
        result = super(GeventRPCClient, self).bind_ports(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def connect(self, url):  #{
        result = super(GeventRPCClient, self).connect(url)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def _reader(self):  #{
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
                    logger.warning(e)
                    break

                logger.debug('received: %r' % msg_list)

                reply = self._parse_reply(msg_list)

                if reply is None:
                    continue

                req_id   = reply['req_id']
                msg_type = reply['type']
                result   = reply['result']

                if msg_type == b'ACK':
                    continue

                async = results.pop(req_id, None)
                if async is None:
                    # result is gone, must be a timeout
                    continue

                if msg_type == b'OK':
                    async.set(result)
                else:
                    async.set_exception(result)

        logger.warning('_reader exited')
    #}
    def shutdown(self):  #{
        """Close the socket and signal the reader greenlet to exit"""
        self._ready = False
        self._exit_ev.set()
        self._ready_ev.set()
        self.socket.close()
        self._ready_ev.clear()
    #}
    def call(self, proc_name, args=[], kwargs={}, ignore=False, timeout=None):  #{
        """
        Call the remote method with *args and **kwargs.

        Parameters
        ----------
        proc_name : <str>   name of the remote procedure to call
        args      : <tuple> positional arguments of the procedure
        kwargs    : <dict>  keyword arguments of the procedure
        ignore    : <bool>  whether to ignore result or wait for it
        timeout   : <float> | None
            Number of seconds to wait for a reply.
            RPCTimeoutError is set as the future result in case of timeout.
            Set to None, 0 or a negative number to disable.

        Returns
        -------
        <object>
            If the call succeeds, the result of the call will be returned.
            If the call fails, `RemoteRPCError` will be raised.
        """
        if not (timeout is None or isinstance(timeout, (int, float))):
            raise TypeError("timeout param: <float> or None expected, got %r" % timeout)

        if not self._ready:
            raise RuntimeError('bind or connect must be called first')

        req_id, msg_list = self._build_request(proc_name, args, kwargs, ignore)

        self.socket.send_multipart(msg_list)

        if ignore:
            return None

        if timeout and timeout > 0:
            def _abort_request():
                result = self._results.pop(req_id, None)
                if result:
                    tout_msg  = "Request %s timed out after %s sec" % (req_id, timeout)
                    logger.debug(tout_msg)
                    result.set_exception(RPCTimeoutError(tout_msg))
            spawn_later(timeout, _abort_request)

        result = AsyncResult()
        self._results[req_id] = result
        return result.get()  # block waiting for a reply passed by ._reader
    #}

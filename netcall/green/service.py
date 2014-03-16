# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
Gevent version of the RPC service

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

import gevent
import zmq

from ..base  import RPCServiceBase
from ..utils import get_zmq_classes


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class GeventRPCService(RPCServiceBase):
    """ An asynchronous RPC service that takes requests over a ROUTER socket.
        Using Gevent compatibility layer from PyZMQ (zmq.green).
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
        Context, _ = get_zmq_classes()

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context

        super(GeventRPCService, self).__init__(**kwargs)

        self.greenlet = None
    #}
    def _create_socket(self):  #{
        super(GeventRPCService, self)._create_socket()
        self.socket = self.context.socket(zmq.ROUTER)
    #}
    def _handle_request(self, msg_list):  #{
        """Handle an incoming request.

        The request is received as a multipart message:

        [<id>..<id>, b'|', req_id, proc_name, <ser_args>, <ser_kwargs>, <ignore>]

        First, the service sends back a notification that the message was
        indeed received:

        [<id>..<id>, b'|', req_id, b'ACK',  service_id]

        Next, the actual reply depends on if the call was successful or not:

        [<id>..<id>, b'|', req_id, b'OK',   <serialized result>]
        [<id>..<id>, b'|', req_id, b'FAIL', <JSON dict of ename, evalue, traceback>]

        Here the (ename, evalue, traceback) are utf-8 encoded unicode.
        """
        req = self._parse_request(msg_list)
        if req is None:
            return
        self._send_ack(req)

        ignore = req['ignore']

        try:
            # raise any parsing errors here
            if req['error']:
                raise req['error']
            # call procedure
            res = req['proc'](*req['args'], **req['kwargs'])
        except Exception:
            not ignore and self._send_fail(req)
        else:
            not ignore and self._send_ok(req, res)
    #}
    def start(self):  #{
        """ Start the RPC service (non-blocking).

            Spawns a receive-reply greenlet that serves this socket.
            Returns spawned greenlet instance.
        """
        assert self.bound or self.connected, 'not bound/connected?'
        assert self.greenlet is None, 'already started'

        def receive_reply():
            while True:
                try:
                    request = self.socket.recv_multipart()
                except:
                    #logger.warning('socket error', exc_info=True)
                    break
                gevent.spawn(self._handle_request, request)
            self.logger.debug('receive_reply exited')

        self.greenlet = gevent.spawn(receive_reply)
        return self.greenlet
    #}
    def stop(self, ):  #{
        """ Stop the RPC service (non-blocking) """
        if self.greenlet is None:
            return  # nothing to do
        bound     = self.bound
        connected = self.connected
        self.logger.debug('resetting the socket')
        self.reset()
        # wait for the greenlet to exit (closed socket)
        self.greenlet.join()
        self.greenlet = None
        # restore bindings/connections
        self.bind(bound)
        self.connect(connected)
    #}
    def shutdown(self):  #{
        """Close the socket and signal the reader greenlet to exit"""
        self.stop()
        self.socket.close(0)
    #}
    def serve(self, greenlets=[]):  #{
        """ Serve RPC requests (blocking)

            Waits for specified greenlets or for this greenlet
        """
        if greenlets:
            return gevent.joinall(greenlets)
        else:
            if self.greenlet is None:
                self.start()
            return self.greenlet.join()
    #}


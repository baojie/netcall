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

from zmq import green

from ..service import RPCServiceBase


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
        assert context is None or isinstance(context, green.Context)
        self.context  = context if context is not None else green.Context.instance()
        self.greenlet = None
        super(GeventRPCService, self).__init__(**kwargs)
    #}
    def _create_socket(self):  #{
        super(GeventRPCService, self)._create_socket()
        self.socket = self.context.socket(zmq.ROUTER)
    #}
    def start(self):  #{
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
    #}
    def stop(self):  #{
        """ Stop the RPC service (non-blocking) """
        raise NotImplementedError("TODO: signal greenlet to quit")
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


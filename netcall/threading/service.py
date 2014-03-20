# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
An RPC service class using ZeroMQ as a transport and
the standard Python threading API for concurrency.

Authors
-------
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

from random  import randint
from Queue   import Queue

import zmq

from ..base  import RPCServiceBase
from ..utils import get_zmq_classes, ThreadPool, logger


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class ThreadingRPCService(RPCServiceBase):
    """ A threading RPC service that takes requests over a ROUTER socket.
    """
    def __init__(self, context=None, pool=None, **kwargs):  #{
        """
        Parameters
        ==========
        context    : <Context>
            An existing ZMQ Context instance, if not passed get_zmq_classes()
            will be used to obtain a compatible Context class.
        pool       : <ThreadPool>
            A thread pool to run handlers in.
        serializer : <Serializer>
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        Context, _ = get_zmq_classes()

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context

        super(ThreadingRPCService, self).__init__(**kwargs)

        self.pool = pool or ThreadPool(128)

        self.io_thread  = None
        self.res_thread = None

        # result drainage
        self.res_queue = Queue(maxsize=self.pool._workers)
        self.res_push  = self.context.socket(zmq.PUSH)
        self.res_addr  = 'inproc://%s-%s' % (
            self.__class__.__name__,
            b'%08x' % randint(0, 0xFFFFFFFF)
        )
        self.res_push.bind(self.res_addr)
    #}
    def _create_socket(self):  #{
        super(ThreadingRPCService, self)._create_socket()
        self.socket = self.context.socket(zmq.ROUTER)
    #}
    def _send_reply(self, reply):  #{
        """ Send a multipart reply to a caller.
            Here we send the reply down the internal res_push socket
            so that an io_thread could send it back to the caller.

            Notice: reply is a list produced by self._build_reply()
        """
        self.res_queue.put(reply)
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

            Spawns two threads:
            - an I/O thread sends/receives ZMQ messages and passes requests to
              the thread pool of handlers
            - a result thread forwards results from req_queue to the I/O thread
              which sends them back to a caller
        """
        assert self.bound or self.connected, 'not bound/connected'
        assert self.io_thread is None and self.res_thread is None, 'already started'

        def res_thread():  #{
            """ Forwards results from res_queue to the res_push socket
                so that an I/O thread could send them back to a caller
            """
            rcv_result = self.res_queue.get
            fwd_result = self.res_push.send_multipart
            try:
                while True:
                    fwd_result(rcv_result())
            except Exception, e:
                logger.error(e, exc_info=True)

            self.res_thread = None  # cleanup
        #}
        def io_thread():  #{
            task_sock = self.socket
            res_pull  = self.context.socket(zmq.PULL)
            res_pull.connect(self.res_addr)

            handle_request = self._handle_request

            _, Poller = get_zmq_classes()
            poller = Poller()
            poller.register(task_sock, zmq.POLLIN)
            poller.register(res_pull,  zmq.POLLIN)
            poll = poller.poll

            try:
                while True:
                    for socket, _ in poll():
                        if socket is task_sock:
                            request = task_sock.recv_multipart()
                            # handle request in a thread-pool
                            self.pool.schedule(handle_request, args=(request,))
                        elif socket is res_pull:
                            result = res_pull.recv_multipart()
                            task_sock.send_multipart(result)
            except Exception, e:
                logger.error(e, exc_info=True)

            # -- cleanup --
            res_pull.close()
            self.io_thread = None
        #}

        self.res_thread = self.pool.schedule(res_thread)
        self.io_thread  = self.pool.schedule(io_thread)

        return self.res_thread, self.io_thread
    #}
    def stop(self):  #{
        """ Stop the RPC service (non-blocking) """
        raise NotImplementedError("TODO: signal threads to quit")
    #}
    def serve(self):  #{
        """ Serve RPC requests (blocking)

            Simply waits for self.res_thread and self.io_thread to exit
        """
        res, io = self.res_thread, self.io_thread
        assert res is not None and io is not None, 'not started'

        while not res.ready or not io.ready:
            res.wait(0.25)
            io.wait(0.25)
    #}


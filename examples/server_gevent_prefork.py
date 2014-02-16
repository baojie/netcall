#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=indent

"""
A simple RPC server that shows how to:

* start several worker processes
* use zmq proxy device to load balance requests to the workers
* make each worker to serve multiple RPC services asynchronously

"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE, distributed as part of this software.
#-----------------------------------------------------------------------------


from gevent import joinall, sleep as gevent_sleep, spawn

from os              import getpid
from multiprocessing import Process, cpu_count

from zmq           import green, ROUTER, DEALER, QUEUE
from netcall.green import GeventRPCService, JSONSerializer


class EchoService(GeventRPCService):

    def echo(self, s):
        print "<pid:%s> %r echo %r" % (getpid(), self.urls, s)
        return s

    def sleep(self, t):
        print "<pid:%s> %r sleep %s" % (getpid(), self.urls, t)
        gevent_sleep(t)

    def error(self):
        raise ValueError('raising ValueError for fun!')

class MathService(GeventRPCService):

    def add(self, a, b):
        print "<pid:%s> %r add %r %r" % (getpid(), self.urls, a, b)
        return a+b

    def subtract(self, a, b):
        print "<pid:%s> %r subtract %r %r" % (getpid(), self.urls, a, b)
        return a-b

    def multiply(self, a, b):
        print "<pid:%s> %r multiply %r %r" % (getpid(), self.urls, a, b)
        return a*b

    def divide(self, a, b):
        print "<pid:%s> %r divide %r %r" % (getpid(), self.urls, a, b)
        return a/b

class Worker(Process):
    def run(self):
        # Multiple RPCService instances can be run in a single process
        # via Greenlets (Gevent cooperative multitasking)

        # Custom serializer/deserializer functions can be passed in. The server
        # side ones must match.
        echo = EchoService(serializer=JSONSerializer())
        echo.connect('ipc:///tmp/rpc-demo-echo.service')

        # We create two Math services to simulate load balancing. A client can
        # connect to both of these services and requests will be load balanced.
        math1 = MathService()
        math1.connect('ipc:///tmp/rpc-demo-math1.service')

        math2 = MathService()
        math2.connect('ipc:///tmp/rpc-demo-math2.service')

        # Next we spawn service greenlets and wait for them to exit
        joinall([
            echo.start(),
            math1.start(),
            math2.start()
        ])



if __name__ == '__main__':
    workers = [Worker() for _ in range(cpu_count())]
    for w in workers:
        w.start()

    context   = green.Context.instance()

    echo_inp  = context.socket(ROUTER)
    math1_inp = context.socket(ROUTER)
    math2_inp = context.socket(ROUTER)

    echo_out  = context.socket(DEALER)
    math1_out = context.socket(DEALER)
    math2_out = context.socket(DEALER)

    echo_inp  .bind('tcp://127.0.0.1:5555')
    math1_inp .bind('tcp://127.0.0.1:5556')
    math2_inp .bind('tcp://127.0.0.1:5557')

    echo_out  .bind('ipc:///tmp/rpc-demo-echo.service')
    math1_out .bind('ipc:///tmp/rpc-demo-math1.service')
    math2_out .bind('ipc:///tmp/rpc-demo-math2.service')

    joinall([
        spawn(green.device, QUEUE, echo_inp,  echo_out),
        spawn(green.device, QUEUE, math1_inp, math1_out),
        spawn(green.device, QUEUE, math2_inp, math2_out),
    ])

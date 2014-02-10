#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""A simple async RPC client that shows how to do load balancing."""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file COPYING.BSD, distributed as part of this software.
#-----------------------------------------------------------------------------

from zpyrpc import TornadoRPCServiceProxy, JSONSerializer
from zmq.eventloop import ioloop

def print_result(r):
    print "Got result:", r

def print_error(ename, evalue, tb):
    print "Got error:", ename, evalue
    print tb

if __name__ == '__main__':
    # Custom serializer/deserializer functions can be passed in. The server
    # side ones must match.
    echo = TornadoRPCServiceProxy(serializer=JSONSerializer())
    echo.connect('tcp://127.0.0.1:5555')
    echo.echo(print_result, print_error, 0, "Hi there")

    echo.error(print_result, print_error, 0)
    # Sleep for 2.0s but timeout after 1000ms.
    echo.sleep(print_result, print_error, 1000, 2.0)

    math = TornadoRPCServiceProxy()
    # By connecting to two instances, requests are load balanced.
    math.connect('tcp://127.0.0.1:5556')
    math.connect('tcp://127.0.0.1:5557')
    for i in range(5):
        for j in range(5):
            math.add(print_result, print_error, 0, i,j)

    loop = ioloop.IOLoop.instance()
    loop.start()

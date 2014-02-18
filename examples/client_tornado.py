#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

""" A simple asynchronous RPC client that shows how to:

    * use specific serializer
    * make multiple asychronous RPC calls at the same time using IOLoop
    * handle remote exceptions
    * do load balancing
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

from netcall       import TornadoRPCClient, JSONSerializer
from zmq.eventloop import ioloop

def print_result(res):
    print "Got result:", res

def print_error(err):
    print "Got error:", err

if __name__ == '__main__':
    # Custom serializer/deserializer functions can be passed in. The server
    # side ones must match.
    echo = TornadoRPCClient(serializer=JSONSerializer())
    echo.connect('tcp://127.0.0.1:5555')
    echo.echo(print_result, print_error, 0, "Hi there")

    echo.error(print_result, print_error, 0)
    echo.call(print_result, print_error, 0, 'error', ignore=True)

    # Sleep for 2.0s but timeout after 1000ms.
    echo.sleep(print_result, print_error, 1000, 2.0)

    math = TornadoRPCClient()
    # By connecting to two instances, requests are load balanced.
    math.connect('tcp://127.0.0.1:5556')
    math.connect('tcp://127.0.0.1:5557')
    for i in range(5):
        for j in range(5):
            math.add(print_result, print_error, 0, i,j)

    loop = ioloop.IOLoop.instance()
    loop.add_timeout(loop.time()+3, loop.stop)
    loop.start()


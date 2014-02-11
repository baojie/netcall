#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""A simple async RPC client that shows how to do load balancing."""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file COPYING.BSD, distributed as part of this software.
#-----------------------------------------------------------------------------

from gevent import spawn, joinall
from zpyrpc import GeventRPCServiceProxy, RemoteRPCError, JSONSerializer

def printer(msg, func, *args):
    "run a function, print results"
    print msg, '<request>'
    res = func(*args)
    print msg, '<response>', res

if __name__ == '__main__':
    # Custom serializer/deserializer functions can be passed in. The server
    # side ones must match.
    echo = GeventRPCServiceProxy(serializer=JSONSerializer())
    echo.connect('tcp://127.0.0.1:5555')

    tasks = [spawn(printer, "[echo] Echoing \"Hi there\"", echo.echo, "Hi there")]
    try:
        echo.error()
    except RemoteRPCError, e:
        print "Got a remote exception:"
        print e.ename
        print e.evalue
        print e.traceback

    tasks.append(spawn(printer, "[echo] Sleeping for 2 seconds...", echo.sleep, 2.0))

    math = GeventRPCServiceProxy()
    # By connecting to two instances, requests are load balanced.
    math.connect('tcp://127.0.0.1:5556')
    math.connect('tcp://127.0.0.1:5557')

    for i in range(5):
        for j in range(5):
            tasks.append(
                spawn(printer, "[math] Adding: %s + %s" % (i, j), math.add, i, j)
            )

    joinall(tasks)

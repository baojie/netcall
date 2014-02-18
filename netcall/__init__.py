# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
NetCall - A simple Python RPC system using ZeroMQ as a transport

Authors:

* Brian Granger
* Alexander Glyzov

Example
-------

To create a simple service::

    from netcall import TornadoRPCService

    echo = TornadoRPCService()

    @echo.task
    def echo(self, s):
        return s

    echo.bind('tcp://127.0.0.1:5555')
    echo.start()
    echo.serve()

To talk to this service::

    from netcall import SyncRPCClient

    p = SyncRPCClient()
    p.connect('tcp://127.0.0.1:5555')
    p.echo('Hi there')
    'Hi there'
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

# Notice: for gevent versions of the classes import zpyrc.green

from .service import TornadoRPCService
from .client  import (
    SyncRPCClient, TornadoRPCClient, AsyncRemoteMethod, RemoteMethod,
    RPCError, RemoteRPCError, RPCTimeoutError
)
from .serializer import *

from sys     import stderr
from logging import getLogger, DEBUG

logger = getLogger('netcall')


def setup_logger(logger=logger, level=DEBUG, stream=stderr):  #{
    """ A utility function to setup a basic logging handler
        for a given logger (netcall by default)
    """
    from logging import StreamHandler, Formatter

    handler   = StreamHandler(stream)
    formatter = Formatter("%(levelname)s:%(name)s:%(message)s")
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(handler)
#}

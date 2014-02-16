# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}
"""
A base class for RPC services and proxies.

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

from abc import ABCMeta, abstractmethod

import zmq
from zmq.eventloop.zmqstream import ZMQStream

from .serializer import PickleSerializer


#-----------------------------------------------------------------------------
# RPC base class
#-----------------------------------------------------------------------------

class RPCBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, serializer=None):  #{
        """Base class for RPC service and proxy.

        Parameters
        ==========
        serializer : Serializer
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        self.socket      = None
        self._ready      = False
        self._serializer = serializer if serializer is not None else PickleSerializer()
        self.reset()
    #}
    @abstractmethod
    def _create_socket(self):  #{
        self._ready = False
    #}

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def reset(self):  #{
        """Reset the socket/stream."""
        if isinstance(self.socket, (zmq.Socket, ZMQStream)):
            self.socket.close()
        self._create_socket()
        self.urls = []
    #}
    def bind(self, url):  #{
        """Bind the service to a url of the form proto://ip:port."""
        self.socket.bind(url)
        self.urls.append(url)
        self._ready = True
    #}
    def bind_ports(self, ip, ports):  #{
        """Try to bind a socket to the first available tcp port.

        The ports argument can either be an integer valued port
        or a list of ports to try. This attempts the following logic:

        * If ports==0, we bind to a random port.
        * If ports > 0, we bind to port.
        * If ports is a list, we bind to the first free port in that list.

        In all cases we save the eventual url that we bind to.

        This raises zmq.ZMQBindError if no free port can be found.
        """
        if isinstance(ports, int):
            ports = [ports]
        for p in ports:
            try:
                if p==0:
                    port = self.socket.bind_to_random_port("tcp://%s" % ip)
                else:
                    self.socket.bind("tcp://%s:%i" % (ip, p))
                    port = p
            except zmq.ZMQError:
                # bind raises this if the port is not free
                continue
            except zmq.ZMQBindError:
                # bind_to_random_port raises this if no port could be found
                continue
            else:
                break
        else:
            raise zmq.ZMQBindError('Could not find an available port')

        url = 'tcp://%s:%i' % (ip, port)
        self.urls.append(url)
        self._ready = True

        return port
    #}
    def connect(self, url):  #{
        """Connect the service to a url of the form proto://ip:port."""
        self.socket.connect(url)
        self.urls.append(url)
        self._ready = True
    #}

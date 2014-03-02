# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
An RPC service using ZeroMQ as a transport.

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

import sys
import traceback

from itertools import chain
from functools import partial
from logging   import getLogger
from abc       import abstractmethod

from zmq.utils               import jsonapi

from .base import RPCBase


logger = getLogger("netcall")


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class RPCServiceBase(RPCBase):  #{

    _RESERVED = ['register','register_object','proc','task','start','stop','serve',
                 'reset', 'connect', 'bind', 'bind_ports'] # From RPCBase

    def __init__(self, *args, **kwargs):  #{
        """
        Parameters
        ==========
        serializer : [optional] <Serializer>
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.

        service_id : [optional] <bytes>
        """
        service_id = kwargs.pop('service_id', None)

        super(RPCServiceBase, self).__init__(*args, **kwargs)

        self.service_id = service_id \
                       or b'%s/%s' % (self.__class__.__name__, self.identity)
        self.procedures = {}  # {<name> : <callable>}

        # register extra class methods as service procedures
        self.register_object(self, restricted=self._RESERVED)
    #}
    def _send_ack(self, request):  #{
        "Send an ACK notification"
        reply = self._build_reply(request, b'ACK', [self.service_id])
        self.socket.send_multipart(reply)
    #}
    def _send_ok(self, request, result):  #{
        "Send a OK reply"
        data_list = self._serializer.serialize_result(result)
        reply = self._build_reply(request, b'OK', data_list)
        self.socket.send_multipart(reply)
    #}
    def _send_fail(self, request):  #{
        """Send a FAIL reply"""
        # take the current exception implicitly
        etype, evalue, tb = sys.exc_info()
        error_dict = {
            'ename'     : str(etype.__name__),
            'evalue'    : str(evalue),
            'traceback' : traceback.format_exc(tb)
        }
        data_list = [jsonapi.dumps(error_dict)]
        reply = self._build_reply(request, b'FAIL', data_list)
        self.socket.send_multipart(reply)
    #}
    def _parse_request(self, msg_list):  #{
        """
        Parse a request
        (should not raise an exception)

        The request is received as a multipart message:

        [<id>..<id>, b'|', req_id, proc_name, <ser_args>, <ser_kwargs>, <ignore>]

        Returns either a None or a dict {
            'route'  : [<id:bytes>, ...],  # list of all dealer ids (a return path)
            'req_id' : <id:bytes>,         # unique message id
            'proc'   : <callable>,         # a task callable
            'args'   : [<arg1>, ...],      # positional arguments
            'kwargs' : {<kw1>, ...},       # keyword arguments
            'ignore' : <bool>,             # ignore result flag
            'error'  : None or <Exception>
        }
        """
        if len(msg_list) < 6 or b'|' not in msg_list:
            logger.error('bad request: %r' % msg_list)
            return None

        error    = None
        args     = None
        kwargs   = None
        ignore   = None
        boundary = msg_list.index(b'|')
        name     = msg_list[boundary+2]
        proc     = self.procedures.get(name, None)
        try:
            data = msg_list[boundary+3:boundary+5]
            args, kwargs = self._serializer.deserialize_args_kwargs(data)
            ignore       = bool(int(msg_list[boundary+5]))
        except Exception, e:
            error = e

        if proc is None:
            error = NotImplementedError("Unregistered procedure %r" % name)

        return dict(
            route  = msg_list[0:boundary],
            req_id = msg_list[boundary+1],
            proc   = proc,
            args   = args,
            kwargs = kwargs,
            ignore = ignore,
            error  = error,
        )
    #}
    def _build_reply(self, request, typ, data):  #{
        """Build a reply message for status and data.

        Parameters
        ----------
        typ : bytes
            Either b'ACK', b'OK' or b'FAIL'.
        data : list of bytes
            A list of data frame to be appended to the message.
        """
        return list(chain(
            request['route'],
            [b'|', request['req_id'], typ],
            data,
        ))
    #}

    @abstractmethod
    def _handle_request(self, msg_list):  #{
        """
        Handle an incoming request.

        The request is received as a multipart message:

        [<id>..<id>, b'|', req_id, proc_name, <serialized args & kwargs>]

        First, the service sends back a notification that the message was
        indeed received:

        [<id>..<id>, b'|', req_id, b'ACK',  service_id]

        Next, the actual reply depends on if the call was successful or not:

        [<id>..<id>, b'|', req_id, b'OK',   <serialized result>]
        [<id>..<id>, b'|', req_id, b'FAIL', <JSON dict of ename, evalue, traceback>]

        Here the (ename, evalue, traceback) are utf-8 encoded unicode.

        Note: subclasses have to override this method
        """
        pass
    #}

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def register(self, func=None, name=None):  #{
        """ A decorator to register a callable as a service task.

            Examples:

            service = TornadoRPCService()

            @service.task
            def echo(s):
                return s

            @service.proc(name='work')
            def do_nothing():
                pass

            service.register(lambda: None, name='dummy')
        """
        if func is None:
            if name is None:
                raise ValueError("at least one argument is required")
            return partial(self.register, name=name)
        else:
            if not callable(func):
                raise ValueError("func argument should be callable")
            if name is None:
                name = func.__name__
            self.procedures[name] = func

        return func
    #}

    task = register  # alias
    proc = register  # alias
    
    def register_object(self, obj, restricted=[], namespace=''):  #{
        """
        Register public functions of a given object as service tasks.
        Give the possibility to not register some restricted functions.
        Give the possibility to prefix the service name with a namespace.
        
        Example 1:
        
        class MyObj(object):
            def __init__(self, value):
                self._value = value
            def value(self):
                return self._value

        first = MyObj(1)
        service.register_object(first)

        second = MyObj(2)
        service.register_object(second, namespace='second')

        third = MyObj(3)
        # Actually register nothing
        service.register_object(third, namespace='third', restricted=['value'])
        
        # Register a full module
        import random
        service.register_object(random, namespace='random')
        
        ...
        
        client.value() # Returns 1
        client.second.value() # Returns 2
        client.third.value() # Exception NotImplementedError
        client.random.randint(10, 30) # Returns an int
        """
        for name in dir(obj):
            if name.startswith('_') or (name in restricted):
                continue
            try:    proc = getattr(obj, name)
            except: continue
            if callable(proc):
                self.procedures['.'.join([namespace, name]).lstrip('.')] = proc
    #}

    @abstractmethod
    def start(self):  #{
        """ Start the service (non-blocking) """
        pass
    #}

    @abstractmethod
    def stop(self):  #{
        """ Stop the service (non-blocking) """
        pass
    #}

    @abstractmethod
    def serve(self):  #{
        """ Serve RPC requests (blocking) """
        pass
    #}
#}

# ZPyRPC = "Zippy RPC" (fork)
(this fork adds Gevent support, refactors code and makes some API changes)

Fast and simple Python RPC based on ZeroMQ and Tornado/Gevent

## Overview

This library provides a small robust RPC system for Python
code using [ZeroMQ](http://zeromq.org/) and
[Tornado](http://www.tornadoweb.org/)/[Gevent](http://www.gevent.org/).
It was originally designed in the context of
[IPython](http://ipython.org/), but eventually
has been spun out into its own project.

Some of the nice features:

* Fast and simple.
* Round robin load balance requests to multiple services.
* Route requests using all of the glory of [ZeroMQ](http://zeromq.org/tutorials:dealer-and-router).
* Both synchronous and asynchronous clients (Tornado or Gevent).
* Set a timeout on RPC calls.
* Run multple services in a single process.
* Pluggable serialization (Pickle [default], JSON, [MessagePack](http://msgpack.org/)).

## Example

To create a simple service:

```python
from zpyrpc import TornadoRPCService, rpc_method

class Echo(TornadoRPCService):
    @rpc_method
    def echo(self, s):
        return s

echo = Echo()
echo.bind('tcp://127.0.0.1:5555')
echo.bind('ipc:///tmp/echo.service')  # multiple endpoints
echo.start()
echo.serve()
```

To talk to this service::

```python
from zpyrpc import GeventRPCClient

p = GeventRPCClient()
p.connect('tcp://127.0.0.1:5555')
p.connect('ipc:///tmp/echo.service')  # auto load balancing
p.echo('Hi there')
'Hi there'
```

See other [examples](https://github.com/aglyzov/zpyrpc/tree/master/examples).


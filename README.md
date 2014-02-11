# ZPyRPC = "Zippy RPC"

Fast and simple Python RPC based on ZeroMQ and Tornado/Gevent

## Overview

This library provides a small robust RPC system for Python
code using ZeroMQ and Tornado/Gevent. It was originally designed
in the context of IPython, but we eventually spun it out into its
own project.

Some of the nice features:

* Fast and simple.
* Round robin load balance requests to multiple services.
* Route requests using all of the glory of ZeroMQ.
* Both synchronous and asynchronous clients (both Gevent and Tornado).
* Set a timeout on RPC calls.
* Run multple services in a single process.
* Pluggable serialization (default is Pickle, JSON and MessagePack are included).

## Example

To create a simple service:

```python
from zpyrpc import TornadoRPCService
class Echo(TornadoRPCService):

    @rpc_method
    def echo(self, s):
        return s

echo = Echo()
echo.bind('tcp://127.0.0.1:5555')
IOLoop.instance().start()
```

To talk to this service::

```python
from zpyrpc import SyncRPCClient
p = SyncRPCClient()
p.connect('tcp://127.0.0.1:5555')
p.echo('Hi there')
'Hi there'
```


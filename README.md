# NetCall -- a simple Python RPC system

This is a simple Python [RPC](http://en.wikipedia.org/wiki/Remote_procedure_call)
system based on [ZeroMQ](http://zeromq.org/tutorials:dealer-and-router)
and [IOLoop](http://zeromq.github.io/pyzmq/api/generated/zmq.eventloop.ioloop.html#zmq.eventloop.ioloop.ZMQIOLoop)/[Gevent](http://www.gevent.org/).

Initially the code was forked from [ZPyRPC](https://github.com/ellisonbg/zpyrpc) in Feb 2014.
The fork has added [Gevent](http://www.gevent.org/) support, refactored code, made incompatible API changes
and added new examples.

## Feature Overview

* Reasonably fast
* Simple hackable code
* Really easy API
* Auto load balancing of multiple services (thanks to ZeroMQ)
* Full [ZeroMQ routing](http://zeromq.org/tutorials:dealer-and-router) as a bonus
* Asynchronous servers (IOLoop or Gevent)
* Both synchronous and asynchronous clients (IOLoop or Gevent)
* Ability to set a timeout on RPC calls
* Ability to run multple services in a single process
* Pluggable serialization (Pickle [default], JSON, [MessagePack](http://msgpack.org/))

## Example

To create a service:

```python
from netcall import TornadoRPCService

echo = TornadoRPCService()

@echo.task
def echo(self, s):
    return s
    
echo.register(lambda n: "Hello %s" % n, name='hello')    

echo.bind('tcp://127.0.0.1:5555')
echo.bind('ipc:///tmp/echo.service')  # multiple endpoints
echo.start()
echo.serve()
```

To talk to this service::

```python
from netcall.green import GeventRPCClient

p = GeventRPCClient()
p.connect('tcp://127.0.0.1:5555')
p.connect('ipc:///tmp/echo.service')  # auto load balancing
p.echo('Hi there')
'Hi there'
p.hello('World')
'Hello World'
```

See other [examples](https://github.com/aglyzov/netcall/tree/master/examples).



[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/aglyzov/netcall/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

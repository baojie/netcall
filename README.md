# NetCall -- a simple Python RPC system

This is a simple Python [RPC](http://en.wikipedia.org/wiki/Remote_procedure_call)
system based on [ZeroMQ](http://zeromq.org/tutorials:dealer-and-router)
and [IOLoop](http://zeromq.github.io/pyzmq/api/generated/zmq.eventloop.ioloop.html#zmq.eventloop.ioloop.ZMQIOLoop)/[Gevent](http://www.gevent.org/).

Initially the code was forked from [ZPyRPC](https://github.com/ellisonbg/zpyrpc) in Feb 2014.
The fork has added [Gevent](http://www.gevent.org/) support, refactored code, made incompatible API changes,
added new features and examples.

## Feature Overview

* Reasonably fast
* Simple hackable code
* Really easy API
* Auto load balancing of multiple services (thanks to ZeroMQ)
* Full [ZeroMQ routing](http://zeromq.org/tutorials:dealer-and-router) as a bonus
* Asynchronous servers (IOLoop or Gevent)
* Both synchronous and asynchronous clients (IOLoop or Gevent)
* Ability to set a timeout on RPC calls
* Ability to run multiple services in a single process
* Pluggable serialization (Pickle [default], JSON, [MessagePack](http://msgpack.org/))
* Support generators (functions yielding) over RPC, including bi-directional capabilities (next(), send(), throw() and close()).

## Example

To create a service:

```python
from netcall.tornado import TornadoRPCService

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

To talk to this service:

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

## Generators

Using generators over RPC will allow you to:
* Push data to a client whenever your service is ready to produce them.
* Control the flow of data whenever the client is ready to receive them.
* Have the client send data (and throw exceptions!) to the service during the communication process.
* Have the client cancel the transmission in the middle.

Generators being part of the Python language itself (yield expression), you can express all of these usages naturally in the code.

Generators are semicoroutines. They might be powerful, they also have some limitations. The communication flow is fully synchronous. For instance, your service cannot prepare its next reply until the client called next() on it. You can see the yield expression and the next(), send(), throw() and close() calls as blocking. However, you can use threads or coroutines on either sides to allow asynchronous processes.

Example of a service yielding:

```python
from netcall.green import GeventRPCService

echo_service = GeventRPCService()

@echo_service.register
def echo(value=None):
    print "Execution starts when 'next()' is called for the first time."
    try:
        while True:
            try:
                value = (yield value)
            except Exception, e:
                value = e
    finally:
        print "Don't forget to clean up when 'close()' is called."
        
echo_service.start().join()
```

Example of a client consuming a generator (S> are print-out from the service):

```python
from netcall.gree import GeventRPCClient

client = GeventRPCClient()

generator = client.echo(1)
print generator.next()
S> Execution starts when 'next()' is called for the first time.
> 1
print generator.next()
> None
print generator.send(2)
> 2
generator.throw(TypeError, "spam")
> TypeError('spam',)
generator = None # implicitly call generator.close()
S> Don't forget to clean up when 'close()' is called.
```

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/aglyzov/netcall/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

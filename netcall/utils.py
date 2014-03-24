# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from __future__ import absolute_import

from sys     import stderr, modules
from imp     import find_module, new_module
from runpy   import _get_module_details
from os.path import join, dirname
from logging import getLogger, DEBUG

from pebble import ThreadPool


logger = getLogger('netcall')
_gevent_cache = {}

#-----------------------------------------------------------------------------
# Utilies
#-----------------------------------------------------------------------------

def import_module(name, cache=modules):  #{
    """Execute a module's code without importing it

       Returns the resulting top level namespace dictionary
    """
    if name in cache:
        return cache[name]

    mod_name, loader, code, fname = _get_module_details(name)
    pkg_name = mod_name.rpartition('.')[0]
    module = new_module(mod_name)
    module.__dict__.update(
        __name__    = mod_name,
        __file__    = fname,
        __loader__  = loader,
        __package__ = pkg_name
    )
    exec code in module.__dict__

    return module
#}
def __import__broken(name, globals=None, locals=None, fromlist=None, cache=modules):  #{
    if name in cache:
        return cache[name]

    parent = None
    module = None
    parts  = name.split('.')

    for i, name in enumerate(parts):
        fqname = '.'.join(parts[:i+1])

        if fqname in cache:
            module = cache[fqname]
        else:
            print fqname, name, parent
            fp, pathname, description = find_module(name, parent and parent.__path__)

            try:
                if fp is None:
                    is_pkg = True
                    pathname = join(pathname, '__init__.py')
                    fp = open(pathname)
                else:
                    is_pkg = False

                module = new_module(fqname)
                module.__name__    = fqname + ('.__main__' if is_pkg else '')
                module.__path__    = [dirname(pathname)]
                module.__file__    = pathname
                module.__loader__  = None
                module.__package__ = fqname if is_pkg else fqname.rpartition('.')[0]

                exec fp in module.__dict__
            finally:
                # Since we may exit via an exception, close fp explicitly.
                fp and fp.close()

            if parent:
                setattr(parent, name, module)

            cache[fqname] = module

        parent = module

    return module
#}
def gevent_patched_module(name, items=None, cache=_gevent_cache):  #{
    if name in cache:
        return cache[name]

    gevent_module = getattr(__import__('gevent.' + name), name)
    module_name = getattr(gevent_module, '__target__', name)
    module = import_module(module_name, cache=_gevent_cache)
    if items is None:
        items = getattr(gevent_module, '__implements__', None)
        if items is None:
            raise AttributeError('%r does not have __implements__' % gevent_module)
    for attr in items:
        setattr(module, attr, getattr(gevent_module, attr))

    return module
#}
def gevent_patched_threading(threading=True, _threading_local=True, Event=True):  #{
    """ Replace the standard :mod:`thread` module to make it greenlet-based.
        If *threading* is true (the default), also patch ``threading``.
        If *_threading_local* is true (the default), also patch ``_threading_local.local``.
    """
    #thread = gevent_patched_module('thread')

    if threading:
        threading = gevent_patched_module('threading')
        if Event:
            from gevent.event import Event
            threading.Event = Event
    if _threading_local:
        _threading_local = __import__('_threading_local')
        from gevent.local import local
        _threading_local.local = local

    return threading
#}

def detect_green_env():  #{
    """ Detects a monkey-patched green thread environment and
        returns either one of these:

        'gevent' | 'eventlet' | 'greenhouse' | None

        Notice, it relies on a monkey-patched threading module.
    """
    import threading
    thr_module = threading._start_new_thread.__module__

    if 'gevent' in thr_module:
        return 'gevent'
    elif 'greenhouse' in thr_module:
        return 'greenhouse'
    elif 'eventlet' in thr_module:
        return 'eventlet'
    else:
        return None
#}
def get_zmq_classes(env=None):  #{
    """ Returns ZMQ Context and Poller classes that are
        compatible with the current environment.

        Tries to detect a monkey-patched green thread environment
        and choses an appropriate Context class.

        Gevent, Eventlet and Greenhouse are supported as well as the
        regular PyZMQ Context class.
    """
    env = env or detect_green_env()

    if env == 'gevent':
        from zmq.green import Context, Poller

    elif env == 'greenhouse':
        import greenhouse
        green = greenhouse.patched('zmq')
        Context, Poller = green.Context, green.Poller

    elif env == 'eventlet':
        from eventlet.green.zmq import Context, Poller

    else:
        from zmq import Context, Poller

    return Context, Poller
#}
def get_green_tools(env=None):  #{
    """ Returns a tuple of callables
          (spawn, spawn_later, Event, AsyncResult)
        compatible with the current green environment.

        Tries to detect a monkey-patched green thread environment
        and choses an appropriate Context class.

        Gevent, Eventlet and Greenhouse are supported.
    """
    env = env or detect_green_env() or 'gevent'

    if env == 'gevent':
        from gevent       import spawn, spawn_later
        from gevent.event import Event
        threading = gevent_patched_threading()
        def Condition(*args, **kwargs):
            return threading._Condition(*args, **kwargs)

    elif env == 'eventlet':
        from eventlet                 import spawn as _spawn, spawn_after
        from eventlet.green.threading import Event, Condition

        def spawn(*ar, **kw):
            g = _spawn(*ar, **kw); g.join = g.wait; return g

        def spawn_later(*ar, **kw):
            g = spawn_after(*ar, **kw); g.join = g.wait; return g

    elif env == 'greenhouse':
        from greenhouse import greenlet, schedule, schedule_in, Event, Condition
        class Greenlet(object):
            def __init__(self, func, *args, **kwargs):
                self.exit_ev = Event()
                def func_ev():
                    try:     func(*args, **kwargs)
                    except:  raise
                    finally: self.exit_ev.set()
                self.greenlet = greenlet(func_ev)
            def wait(self, *args, **kwargs):
                return self.exit_ev.wait(*args, **kwargs)
            def spawn(self, after=None):
                if after is None:
                    schedule(self.greenlet)
                else:
                    schedule_in(after, self.greenlet)
                return self
            # aliases
            join = wait
            run  = spawn

        def spawn(func, *args, **kwargs):
            g = Greenlet(func, *args, **kwargs)
            return g.spawn()

        def spawn_later(sec, func, *args, **kwargs):
            g = Greenlet(func, *args, **kwargs)
            return g.spawn(after=sec)

    else:
        raise ValueError('unsupported green environment %r' % env)

    return spawn, spawn_later, Event, Condition
#}

def setup_logger(logger='netcall', level=DEBUG, stream=stderr):  #{
    """ A utility function to setup a basic logging handler
        for a given logger (netcall by default)
    """
    from logging import StreamHandler, Formatter

    if isinstance(logger, basestring):
        logger = getLogger(logger)

    handler   = StreamHandler(stream)
    formatter = Formatter("[%(process)s/%(threadName)s]:%(levelname)s:%(name)s:%(funcName)s():%(message)s")
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger
#}

class RemoteMethodBase(object):  #{
    """A remote method class to enable a nicer call syntax."""

    def __init__(self, client, method):
        self.client = client
        self.method = method
#}
class RemoteMethod(RemoteMethodBase):  #{

    def __call__(self, *args, **kwargs):  #{
        return self.client.call(self.method, args, kwargs)
    #}

    def __getattr__(self, name):  #{
        return RemoteMethod(self.client, '.'.join([self.method, name]))
    #}
#}


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from __future__ import absolute_import

from sys     import stderr
from logging import getLogger, DEBUG

from pebble import ThreadPool


logger = getLogger('netcall')

#-----------------------------------------------------------------------------
# Utilies
#-----------------------------------------------------------------------------

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


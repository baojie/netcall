#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from netcall       import ThreadingRPCService, SyncRPCClient
from netcall.utils import setup_logger, get_zmq_classes

if __name__ == "__main__":
    setup_logger()

    #service = ThreadingRPCService()
    #service.bind('tcp://*:12345')
    #service.start()

    Context, _ = get_zmq_classes()
    context = Context()
    client = SyncRPCClient(context=context)
    client.bind(['ipc://test-1', 'ipc://test-2', 'ipc://test-3'])

    print client.bound
    print client.connected


    #service.shutdown()

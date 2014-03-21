# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from netcall.green import GeventRPCClient, GeventRPCService

from .base          import BaseCase
from .client_mixins import ClientBindConnectMixIn
from .rpc_mixins    import RPCCallsMixIn


class GeventBase(BaseCase):

    def setUp(self):
        from zmq.green import Context

        self.context = Context()
        self.client  = GeventRPCClient(context=self.context)
        self.service = GeventRPCService(context=self.context)

        super(GeventBase, self).setUp()

    def tearDown(self):
        self.client.shutdown()
        self.service.shutdown()
        self.context.term()

        super(GeventBase, self).tearDown()


class GeventClientBindConnectTest(ClientBindConnectMixIn, GeventBase):
    pass

class GeventRPCCallsTest(RPCCallsMixIn, GeventBase):
    pass


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from gevent import monkey
monkey.patch_all()

from os       import removedirs
from tempfile import mkdtemp
from unittest import TestCase

from netcall.green import GeventRPCClient, GeventRPCService

from test_client_base import BaseClientTest
from test_rpc_base import BaseRPCTest

class GeventClientTest(BaseClientTest, TestCase):

    def setUp(self):
        self.client = GeventRPCClient()
        super(GeventClientTest, self).setUp()

class GeventRPCTest(BaseRPCTest, TestCase):

    def setUp(self):
        self.client = GeventRPCClient()
        self.service = GeventRPCService()
        super(GeventRPCTest, self).setUp()

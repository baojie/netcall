# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from os       import removedirs
from tempfile import mkdtemp
from unittest import TestCase

from netcall  import SyncRPCClient
#from netcall.green import GeventRPCService # We don't have a simple "sync" service

from test_client_base import BaseClientTest
from test_rpc_base import BaseRPCTest


class SyncClientTest(BaseClientTest, TestCase):

    def setUp(self):
        self.client = SyncRPCClient()
        super(SyncClientTest, self).setUp()

#class SyncRPCTest(BaseRPCTest, TestCase):
#
#    def setUp(self):
#        self.client = SyncRPCClient()
#        self.service = GeventRPCService()
#        super(SyncRPCTest, self).setUp()
        


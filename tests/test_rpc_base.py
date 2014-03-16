from os       import removedirs
from tempfile import mkdtemp

from netcall import RemoteRPCError

class BaseRPCTest(object):

    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = mkdtemp(prefix='netcall-test-')
        cls.url    = 'ipc://%s/test-%s' % (cls.tmp_dir, 0)

    @classmethod
    def tearDownClass(cls):
        removedirs(cls.tmp_dir)

    def setUp(self):
        # Setup of self.client and self.service by child classes
        assert self.client
        assert self.service
        
        self.service.bind(self.url)
        self.client.connect(self.url)

    def tearDown(self):
        self.client.socket.close(0)
        self.service.socket.close(0)
        
    def assertNotImplementedRemotely(self, func_name):
        with self.assertRaisesRegexp(RemoteRPCError, "NotImplementedError: Unregistered procedure '%s'" % func_name):
            self.client.call(func_name)
            
            
    reserved_fields = [
        'register','register_object','proc','task','start','stop','serve',
        'reset', 'connect', 'bind', 'bind_ports']
        
    def test_netcall_reserved(self):
        self.service.start()
        
        for f in self.reserved_fields:
            self.assertNotImplementedRemotely(f)
        
    def test_cannot_register_netcall_reserved(self):
        def dummy():
            pass
        for f in self.reserved_fields:
            with self.assertRaisesRegexp(ValueError, '{} is a reserved function name'.format(f)):
                self.service.register(dummy, name=f)
                 
        self.service.start()
        
        self.assertDictEqual(self.service.procedures, {})
        for f in self.reserved_fields:
            self.assertNotImplementedRemotely(f)
        
    def test_cannot_register_object_netcall_reserved(self):
        def dummy():
            pass
        class Dummy(object):
            pass
        toy = Dummy()
        for f in self.reserved_fields:
            setattr(toy, f, dummy)
        self.service.register_object(toy)
                 
        self.service.start()
        
        self.assertDictEqual(self.service.procedures, {})
        for f in self.reserved_fields:
            self.assertNotImplementedRemotely(f)
        
    def test_function(self):
        @self.service.register
        def fixture():
            return 'This is a test'
            
        self.service.start()
        
        self.assertEqual(self.client.fixture(), 'This is a test')
        
    def test_function_named(self):
        @self.service.register(name='work')
        def fixture():
            return 'This is a test'
            
        self.service.start()
        
        self.assertEqual(self.client.work(), 'This is a test')
        self.assertNotImplementedRemotely("fixture")
            
    def test_function_args(self):
        @self.service.register
        def fn_double_one_arg(arg1):
            return arg1 * 2
            
        @self.service.register
        def fn_mul_two_args(arg1, arg2):
            return arg1 * arg2
            
        @self.service.register
        def fn_mul_vargs(*args):
            mul = 1
            for arg in args:
                mul = mul * arg
            return mul
            
        self.service.start()
            
        self.assertEqual(self.client.fn_double_one_arg(7), 14)
        self.assertEqual(self.client.fn_mul_two_args(7, 3), 21)
        self.assertEqual(self.client.fn_mul_vargs(7, 3, 10), 210)
            
    def test_function_kwargs(self):
        @self.service.register
        def fn_double_one_kwarg(arg1=None):
            return arg1 * 2
            
        @self.service.register
        def fn_mul_two_kwargs(arg1=None, arg2=None):
            return arg1 * arg2
            
        @self.service.register
        def fn_mul_vkwargs(**kwargs):
            mul = 1
            for arg in kwargs.values():
                mul = mul * arg
            return mul
            
        self.service.start()
            
        self.assertEqual(self.client.fn_double_one_kwarg(arg1=7), 14)
        self.assertEqual(self.client.fn_mul_two_kwargs(arg1=7, arg2=3), 21)
        self.assertEqual(self.client.fn_mul_vkwargs(arg1=7, arg2=3, arg3=10), 210)
        
        with self.assertRaisesRegexp(RemoteRPCError, "TypeError: .*() got an unexpected keyword argument 'argNot'"):
            self.client.fn_double_one_kwarg(argNot=7)
        with self.assertRaisesRegexp(RemoteRPCError, "TypeError: .*() got an unexpected keyword argument 'argNot'"):
            self.client.fn_mul_two_kwargs(arg1=7, arg2=3, argNot=17)
            
    def test_function_args_kwargs(self):
        @self.service.register
        def fn_one_arg_one_kwarg(arg1, arg2=None):
            return arg1 * arg2
            
        @self.service.register
        def fn_vargs_vkwargs(*args, **kwargs):
            return args[0] * sorted(kwargs.items())[0][1]
            
        self.service.start()
        
        self.assertEqual(self.client.fn_one_arg_one_kwarg(7, arg2=3), 21)
        self.assertEqual(self.client.fn_vargs_vkwargs(7, 5, argA=3, argB=18), 21)
        
    def test_object(self):
        toy = ToyObject(12)
        self.service.register_object(toy)
        self.service.start()
        
        self.assertEqual(self.client.value(), toy.value())
        self.assertIsNone(self.client.restricted())
        
    def test_object_private(self):
        toy = ToyObject(12)
        self.service.register_object(toy)
        self.service.start()
        
        self.assertNotImplementedRemotely("_private")
        
    def test_object_restricted(self):
        toy = ToyObject(12)
        self.service.register_object(toy, restricted=['restricted'])
        self.service.start()
        
        self.assertNotImplementedRemotely("restricted")
        
    def test_object_namespace_one_level(self):
        toys = []
        for i, n in enumerate('abc'):
            toys.append(ToyObject(i))
            self.service.register_object(toys[i], namespace=n)
            
        self.service.start()
        
        self.assertNotImplementedRemotely('value')
        self.assertEqual(self.client.a.value(), toys[0].value())
        self.assertEqual(self.client.b.value(), toys[1].value())
        self.assertEqual(self.client.c.value(), toys[2].value())
        
    def test_object_namespace_n_levels(self):
        toy = ToyObject(12)
        self.service.register_object(toy, namespace='this.has.a.toy')
        
        self.service.start()
        
        self.assertEqual(self.client.this.has.a.toy.value(), toy.value())
        
    def test_object_module(self):
        import random
        self.service.register_object(random)
        
        self.service.start()
        
        self.assertIsInstance(self.client.randint(0, 10), int)
        self.assertIsInstance(self.client.random(), float)
    
class ToyObject(object):

    def __init__(self, value):
        self._value = value
        
    def value(self):
        return self._value
        
    def _private(self):
        pass
        
    def restricted(self):
        pass

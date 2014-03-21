# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from netcall import RemoteRPCError


class RPCCallsMixIn(object):  #{

    def setUp(self):
        super(RPCCallsMixIn, self).setUp()

        assert self.client
        assert self.service

        self.url = self.urls[0]
        self.service.bind(self.url)
        self.client.connect(self.url)


    def assertNotImplementedRemotely(self, func_name):
        err_msg = "NotImplementedError: Unregistered procedure '%s'" % func_name
        with self.assertRaisesRegexp(RemoteRPCError, err_msg):
            self.client.call(func_name)

    def test_netcall_restricted(self):
        self.service.start()
        restricted_fields = [
            'register','register_object','proc','task',
            'start','stop','serve','shutdown',
            'reset','connect','bind','bind_ports'
        ]
        for f in restricted_fields:
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
            return args[0] * kwargs.values()[0]

        self.service.start()

        self.assertEqual(self.client.fn_one_arg_one_kwarg(7, arg2=3), 21)
        self.assertEqual(self.client.fn_vargs_vkwargs(7, arg2=3), 21)

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
        self.assertEqual(self.client.a.value(), 0)
        self.assertEqual(self.client.b.value(), 1)
        self.assertEqual(self.client.c.value(), 2)

    def test_object_namespace_n_levels(self):
        toy = ToyObject(12)
        self.service.register_object(toy, namespace='this.has.a.toy')

        self.service.start()

        self.assertEqual(self.client.this.has.a.toy.value(), 12)

    def test_object_module(self):
        import random
        self.service.register_object(random)

        self.service.start()

        self.assertIsInstance(self.client.randint(0, 10), int)
        self.assertIsInstance(self.client.random(), float)
#}

class ToyObject(object):  #{

    def __init__(self, value):
        self._value = value

    def value(self):
        return self._value

    def _private(self):
        pass

    def restricted(self):
        pass
#}

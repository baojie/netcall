from setuptools import setup, find_packages

setup(
    name = "zpyrpc",
    version = "0.2",
    packages = find_packages(),

    install_requires = ['tornado','pyzmq','gevent'],

    author = "Brian Granger",
    author_email = "ellisonbg@gmail.com",
    description = "Zippy fast and simple Python RPC based on ZeroMQ and Tornado/Gevent",
    license = "Modified BSD",
    keywords = "ZeroMQ Tornado Gevent PyZMQ",
    url = "http://github.com/ellisonbg/zpyrpc",
)


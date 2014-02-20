#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name = "netcall",
    version = "0.2.4",
    packages = find_packages(),

    install_requires = ['pyzmq','tornado'],

    author = "Alexander Glyzov",
    author_email = "bonoba@gmail.com",
    description = "A simple Python RPC system (ZeroMQ + Tornado/Gevent)",
    license = "Modified BSD",
    keywords = "ZeroMQ ZMQ PyZMQ Tornado Gevent RPC async",
    url = "http://github.com/aglyzov/netcall",
)


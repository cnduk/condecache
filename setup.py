#!/usr/bin/env python
from setuptools import setup, find_packages

from condecache import __version__

setup(
    name='condecache',
    version=__version__,
    author='Conde Nast Digital UK',
    author_email='condenet.technical@condenast.co.uk',
    description="caching framework to reduce boilerplate",
    packages=find_packages(),
    install_requires=[],
    tests_require=['mock'],
    url="https://github.com/cnduk/condecache",
    download_url="https://github.com/cnduk/condecache/tarball/v{}".format(__version__),
    keywords=[],
    license='MIT',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
    ],
)
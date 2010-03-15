#!/usr/bin/env python
# encoding: utf-8
"""
setup.py

Created by Kurtiss Hare on 2010-03-14.
"""

from distutils.core import setup
from monque import __version__ as version
 
dependencies = ["pymongo"]
 
setup(
    name                = 'monque',
    version             = version,
    description         = 'Persistent job queueing library using MongoDB, inspired by Dreque',
    author              = 'Kurtiss Hare',
    author_email        = 'kurtiss@kurtiss.org',
    url                 = 'http://github.com/kurtiss/monque',
    packages            = ['monque'],
    requires            = dependencies,
    install_requires    = dependencies,
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
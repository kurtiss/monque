#!/usr/bin/env python
# encoding: utf-8
"""
__init__.py

Created by Kurtiss Hare on 2010-03-12.
"""


from version import VERSION
from base import Monque
from worker import MonqueWorker
from job import MonqueJob, job

__version__ = VERSION
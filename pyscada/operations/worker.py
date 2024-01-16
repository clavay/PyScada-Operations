#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from pyscada.utils.scheduler import SingleDeviceDAQProcessWorker
from . import PROTOCOL_ID_2 as PROTOCOL_ID

import logging


logger = logging.getLogger(__name__)


class Process(SingleDeviceDAQProcessWorker):
    device_filter = dict(aggregateddevice__isnull=False, protocol_id=PROTOCOL_ID)
    bp_label = "pyscada.aggregated-%s"

    def __init__(self, dt=5, **kwargs):
        super(SingleDeviceDAQProcessWorker, self).__init__(dt=dt, **kwargs)

# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from pyscada.device import GenericDevice
from .devices import GenericDevice as GenericHandlerDevice

try:
    import numpy as np

    driver_ok = True
except ImportError:
    driver_ok = False

from time import time
import logging

logger = logging.getLogger(__name__)


class Device(GenericDevice):
    def __init__(self, device):
        self.driver_ok = driver_ok
        self.handler_class = GenericHandlerDevice
        super().__init__(device)

        for var in self.device.variable_set.filter(active=1):
            if not hasattr(var, "sgetiersvariable"):
                continue
            self.variables[var.pk] = var

        if self.driver_ok and self.driver_handler_ok:
            self._h.connect()
        else:
            logger.warning(f"Cannot import numpy or handler for {self.device}")

    def write_data(self, variable_id, value, task):
        """
        write value to the instrument/device
        """
        output = []
        logger.warning(f"Aggregated device cannot write.")
        return output

    def request_data(self):
        """
        request data from the instrument/device
        """
        output = []
        if not self.driver_ok:
            logger.warning("Cannot import numpy")
            return output

        output = super().request_data()
        return output

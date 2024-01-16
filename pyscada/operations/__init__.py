# -*- coding: utf-8 -*-
from __future__ import unicode_literals

__version__ = "0.8.1"
__author__ = "Camille Lavayssiere"
__email__ = "team@pyscada.org"
__description__ = "Operations extension for PyScada a Python and Django based Open Source SCADA System"
__app_name__ = "Operations"

# operations
PROTOCOL_ID = 18

# aggregated
PROTOCOL_ID_2 = 19
aggretation_name = "Aggregated"

parent_process_list = [
    {
        "pk": PROTOCOL_ID_2,
        "label": "pyscada." + aggretation_name.lower(),
        "process_class": "pyscada." + __app_name__.lower() + ".worker.Process",
        "process_class_kwargs": '{"dt_set":30}',
        "enabled": True,
    }
]

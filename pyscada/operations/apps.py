# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _
from django.db.utils import ProgrammingError, OperationalError

from . import __app_name__


class PyScadaOperationsConfig(AppConfig):
    name = "pyscada." + __app_name__.lower()
    verbose_name = _("PyScada " + __app_name__)
    path = os.path.dirname(os.path.realpath(__file__))
    default_auto_field = "django.db.models.AutoField"

    def ready(self):
        from . import signals

        try:
            from pyscada.models import DataSourceModel, DataSource
            from .models import OperationsDataSource

            dsm, _ = DataSourceModel.objects.update_or_create(
                inline_model_name="OperationsDataSource",
                defaults={
                    "name": "Operations data source",
                    "can_add": False,
                    "can_change": False,
                    "can_select": True,
                },
            )

            ds, _ = DataSource.objects.get_or_create(datasource_model=dsm)

            ldse, _ = OperationsDataSource.objects.get_or_create(datasource=ds)
        except ProgrammingError:
            pass
        except OperationalError:
            pass

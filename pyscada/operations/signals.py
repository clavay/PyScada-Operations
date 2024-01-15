# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from pyscada.models import Device, Variable
from .models import OperationsDataSource

from django.dispatch import receiver
from django.db.models.signals import post_save

import logging

logger = logging.getLogger(__name__)


@receiver(post_save, sender=Variable)
def _reinit_daq_daemons(sender, instance, **kwargs):
    """
    update the daq daemon configuration when changes be applied in the models
    """
    if type(instance) is Variable:
        if hasattr(instance, "operationsvariable"):
            if not OperationsDataSource.objects.all().count():
                logger.warning("OperationsDataSource is missing !")
            elif instance.datasource != OperationsDataSource.objects.first().datasource:
                instance.datasource = OperationsDataSource.objects.first().datasource
                Variable.objects.bulk_update([instance], ["datasource"])
                logger.info(f"Update {instance} datasource to OperationsDataSource")

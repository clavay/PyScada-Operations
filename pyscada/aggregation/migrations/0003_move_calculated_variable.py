# -*- coding: utf-8 -*-
# Generated by Django 1.10.2 on 2017-02-24 12:49
from __future__ import unicode_literals
from .. import PROTOCOL_ID, __app_name__

from django.db import migrations
from django.db.utils import ProgrammingError
from django.apps import apps as global_apps
from django.db.migrations.recorder import MigrationRecorder

import logging

logger = logging.getLogger(__name__)


def forwards_func(apps, schema_editor):
    # We get the model from the versioned app registry;
    # if we directly import it, it'll be the wrong version
    try:
        Device = apps.get_model("pyscada", "Device")
        AggregationDevice = apps.get_model("aggregation", "AggregationDevice")
        DeviceProtocol = apps.get_model("pyscada", "DeviceProtocol")
        Variable = apps.get_model("pyscada", "Variable")
        AggregationVariable = apps.get_model("aggregation", "AggregationVariable")
        CalculatedVariableSelector = apps.get_model("pyscada", "CalculatedVariableSelector")
        CalculatedVariable = apps.get_model("pyscada", "CalculatedVariable")
        PeriodFieldOld = apps.get_model("pyscada", "PeriodicField")
        PeriodFieldNew = apps.get_model("aggregation", "PeriodicField")
        db_alias = schema_editor.connection.alias

        agg_protocol = DeviceProtocol.objects.using(db_alias).get(id=PROTOCOL_ID)

        # create devices
        for cvs in CalculatedVariableSelector.objects.using(db_alias).all():
            f_ad = AggregationDevice.objects.using(db_alias).filter(
                variable=cvs.main_variable
            )
            if not f_ad.count():
                d, _ = Device.objects.using(db_alias).get_or_create(
                    short_name=f"Aggregation_{cvs.main_variable.name}",
                    protocol=agg_protocol,
                    description=f"Aggregation variable for {cvs.main_variable.name}",
                    polling_interval=60,  # 1 minute
                )
                ad, _ = AggregationDevice.objects.using(db_alias).get_or_create(
                    aggregation_device=d, variable=cvs.main_variable
                )
            else:
                ad = f_ad.first()
                d = ad.aggregation_device

            # create variables
            for cv in cvs.calculatedvariable_set.all():
                p_old = cv.period
                p_new, _ = PeriodFieldNew.objects.using(db_alias).get_or_create(
                    type=p_old.type,
                    property=p_old.property,
                    start_from=p_old.start_from,
                    period=p_old.period,
                    period_factor=p_old.period_factor,
                )
                f_av = AggregationVariable.objects.using(db_alias).filter(
                    aggregation_variable__device=d, period=p_new
                )
                if not f_av.count():
                    v = cv.store_variable
                    v.id = None
                    v.name = f"aggregation_{v.name}"
                    v.description = f"Aggregation variable for {p_new.get_type_display()} {p_new.property} {p_new.start_from} {p_new.period} {p_new.period_factor}"
                    v.device = d
                    v.save()
                    av, _ = AggregationVariable.objects.using(db_alias).get_or_create(
                        aggregation_variable=v, period=p_new
                    )
                    logger.info(
                        f"Created AggregationVariable {av.aggregation_variable.name}"
                    )
                else:
                    av = f_av.first()
                    v = av.aggregation_variable
    except (ProgrammingError, LookupError):
        pass

def reverse_func(apps, schema_editor):
    # forwards_func() creates two Country instances,
    # so reverse_func() should delete them.
    Device = apps.get_model("pyscada", "Device")
    AggregationDevice = apps.get_model("aggregation", "AggregationDevice")
    DeviceProtocol = apps.get_model("pyscada", "DeviceProtocol")
    Variable = apps.get_model("pyscada", "Variable")
    AggregationVariable = apps.get_model("aggregation", "AggregationVariable")
    CalculatedVariableSelector = apps.get_model("pyscada", "CalculatedVariableSelector")
    CalculatedVariable = apps.get_model("pyscada", "CalculatedVariable")
    PeriodFieldOld = apps.get_model("pyscada", "PeriodicField")
    PeriodFieldNew = apps.get_model("aggregation", "PeriodicField")
    db_alias = schema_editor.connection.alias

    # create devices
    try:
        for ad in AggregationDevice.objects.using(db_alias).all():
            f_cvs = CalculatedVariableSelector.objects.using(db_alias).filter(
                main_variable=ad.variable
            )
            if not f_cvs.count():
                cvs, _ = CalculatedVariableSelector.objects.using(db_alias).get_or_create(
                    main_variable=ad.variable,
                )
            else:
                cvs = f_cvs.first()
            for v in ad.aggregation_device.variable_set.all():
                p_new = v.aggregationvariable.period
                p_old, _ = PeriodFieldOld.objects.using(db_alias).get_or_create(
                    type=p_new.type,
                    property=p_new.property,
                    start_from=p_new.start_from,
                    period=p_new.period,
                    period_factor=p_new.period_factor,
                )
                cvs.period_fields.add(p_old)
                logger.info(
                    f"Recreated CalculatedVariableSelector period field {p_old.get_type_display()} {p_old.property} {p_old.start_from} {p_old.period} {p_old.period_factor}"
                )
            cvs.save()
    except ProgrammingError:
        pass

class Migration(migrations.Migration):
    dependencies = [
        ("aggregation", "0002_initial"),
    ]
    run_before = [
    ]

    if global_apps.is_installed("pyscada") and not MigrationRecorder.Migration.objects.filter(app="pyscada", name__contains="0108_remove_calculatedvariable_period_and_more").count():
        print("add run_before")
        run_before.append(("pyscada", "0108_remove_calculatedvariable_period_and_more"))

    operations = [
        migrations.RunPython(forwards_func, reverse_func),
    ]
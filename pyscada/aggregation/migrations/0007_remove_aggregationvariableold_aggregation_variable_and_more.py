# Generated by Django 4.2.5 on 2024-02-05 15:30

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("aggregation", "0006_invert_device_and_variable"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="aggregationvariableold",
            name="aggregation_variable",
        ),
        migrations.RemoveField(
            model_name="aggregationvariableold",
            name="period",
        ),
        migrations.DeleteModel(
            name="AggregationDeviceOld",
        ),
        migrations.DeleteModel(
            name="AggregationVariableOld",
        ),
        migrations.DeleteModel(
            name="PeriodicField",
        ),
    ]

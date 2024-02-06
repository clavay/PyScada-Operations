# Generated by Django 4.2.5 on 2024-02-06 10:00

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        (
            "aggregation",
            "0007_remove_aggregationvariableold_aggregation_variable_and_more",
        ),
    ]

    operations = [
        migrations.AddField(
            model_name="aggregationdevice",
            name="timestamp_offset",
            field=models.BigIntegerField(
                default=0,
                help_text="Offset in seconds used to save the aggregated value from the period start. If negative, the saved data will be in the past; if positive, it will be in the future.",
            ),
        ),
    ]
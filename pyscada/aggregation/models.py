from django.db import models
from django.forms.models import BaseInlineFormSet
from django.core.exceptions import ValidationError
from django.utils.safestring import mark_safe
from django.utils.timezone import now
from django import forms
from django.utils.timezone import now, make_aware, is_naive

from pyscada.models import (
    DataSource,
    DataSourceModel,
    Variable,
    Device,
)

from time import time
from datetime import datetime, date
from dateutil import relativedelta
from monthdelta import monthdelta
import logging

logger = logging.getLogger(__name__)

def validate_nonzero(value):
    if value == 0:
        raise ValidationError(
            _("Quantity %(value)s is not allowed"),
            params={"value": value},
        )


def start_from_default():
    return make_aware(
        datetime.combine(date.today(), datetime.min.time())
    )


class PeriodicField(models.Model):
    """
    Auto calculate and store value related to a Variable for a time range.
    Example: - store the min of each month.
    - store difference of each day between 9am an 8:59am
    """

    type_choices = (
        (0, "min"),
        (1, "max"),
        (2, "total"),
        (3, "difference"),
        (4, "difference percent"),
        (5, "delta"),
        (6, "mean"),
        (7, "first"),
        (8, "last"),
        (9, "count"),
        (10, "count value"),
        (11, "range"),
        (12, "step"),
        (13, "change count"),
        (14, "distinct count"),
    )
    type = models.SmallIntegerField(
        choices=type_choices,
        help_text="Min: Minimum value of a field<br>"
        "Max: Maximum value of a field<br>"
        "Total: Sum of all values in a field<br>"
        "Difference: Difference between first and last value of a field<br>"
        "Difference percent: Percentage change between "
        "first and last value of a field<br>"
        "Delta: Cumulative change in value, only counts increments<br>"
        "Mean: Mean value of all values in a field<br>"
        "First: First value in a field<br>"
        "Last: Last value in a field<br>"
        "Count: Number of values in a field<br>"
        "Count value: Number of a value in a field<br>"
        "Range: Difference between maximum and minimum values of a field<br>"
        "Step: Minimal interval between values of a field<br>"
        "Change count: Number of times the field’s value changes<br>"
        "Distinct count: Number of unique values in a field",
    )
    property = models.CharField(
        default="",
        blank=True,
        null=True,
        max_length=255,
        help_text="Min: superior or equal this value, ex: 53.5 "
        "(use >53.5 for strictly superior)<br>"
        "Max: lower or equal this value, ex: 53.5 "
        "(use <53.5 for strictly lower)<br>"
        "Count value : enter the value to count",
    )
    start_from = models.DateTimeField(
        default=start_from_default,
        help_text="Calculate from this DateTime and then each period_factor*period",
    )
    period_choices = (
        (0, "second"),
        (1, "minute"),
        (2, "hour"),
        (3, "day"),
        (4, "week"),
        (5, "month"),
        (6, "year"),
    )
    period = models.SmallIntegerField(choices=period_choices)
    period_factor = models.PositiveSmallIntegerField(
        default=1,
        validators=[validate_nonzero],
        help_text="Example: set to 2 and choose minute to have a 2 minutes period",
    )

    def __str__(self):
        s = self.type_choices[self.type][1] + "-"
        if self.property != "" and self.property is not None:
            s += str(self.property).replace("<", "lt").replace(">", "gt") + "-"
        s += str(self.period_factor) + self.period_choices[self.period][1]
        if self.period_factor > 1:
            s += "s"
        s += "-from:" + str(self.start_from.date()) + "T" + str(self.start_from.time())
        return s

    def validate_unique(self, exclude=None):
        qs = PeriodicField.objects.filter(
            type=self.type,
            property=self.property,
            start_from=self.start_from,
            period=self.period,
            period_factor=self.period_factor,
        ).exclude(id=self.id)
        if len(qs):
            raise ValidationError("This periodic field already exist.")


class AggregationVariable(models.Model):
    aggregation_variable = models.OneToOneField(Variable, on_delete=models.CASCADE)
    period = models.ForeignKey(PeriodicField, on_delete=models.CASCADE)
    last_check = models.DateTimeField(blank=True, null=True)
    state = models.CharField(blank=True, null=True, max_length=100)

    class FormSet(BaseInlineFormSet):
        def add_fields(self, form, index):
            super().add_fields(form, index)
            if not form.initial:
                form.fields["last_check"].widget = forms.HiddenInput()
                form.fields["state"].widget = forms.HiddenInput()
            else:
                form.fields["last_check"].widget = forms.TextInput()
                form.fields["last_check"].disabled = True
                form.fields["last_check"].widget.attrs["size"] = 70
                form.fields["state"].widget = forms.TextInput()
                form.fields["state"].widget.attrs["size"] = 70
                form.fields["state"].disabled = True

    def clean(self):
        super().clean()
        if self.period is None:
            raise ValidationError("Select a period.")

    def has_changed(self):
        return True

    @property
    def main_variable(self):
        try:
            return self.aggregation_variable.device.aggregationdevice.variable
        except Exception as e:
            logger.warning(e)
            return None

    @property
    def parent_variable(self):
        try:
            return self.aggregation_variable
        except:
            return None

    def __str__(self):
        if self.parent_variable is not None:
            return self.parent_variable.name
        return "EmptyAggregationVariable"


class AggregationDevice(models.Model):
    aggregation_device = models.OneToOneField(Device, on_delete=models.CASCADE)
    variable = models.ForeignKey(
        Variable, on_delete=models.CASCADE, blank=True, null=True
    )

    def clean(self):
        super().clean()
        if self.variable is None:
            raise ValidationError("Select a variable to aggregate.")

    def parent_device(self):
        try:
            return self.aggregation_device
        except:
            return None

    def __str__(self):
        if self.parent_device() is not None:
            return self.parent_device().short_name
        return "EmptyAggregationDevice"

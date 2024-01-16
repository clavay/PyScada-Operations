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
from datetime import datetime, timedelta
from dateutil import relativedelta
from monthdelta import monthdelta
import logging

logger = logging.getLogger(__name__)


try:
    import simpleeval

    driver_ok = True
except ImportError:
    driver_ok = False

from time import time

import logging

logger = logging.getLogger(__name__)

global_time_min = None
global_time_max = None
global_time_max_excluded = True


def get_variable_value(variable_id, use_date_saved=False, query_type="value"):
    logger.debug(
        f"{variable_id} {global_time_min} {global_time_max} {use_date_saved} {query_type}"
    )
    kwargs = {
        "time_min": global_time_min,
        "time_max": global_time_max,
        "use_date_saved": use_date_saved,
        "time_max_excluded": global_time_max_excluded,
    }
    try:
        v = Variable.objects.get(id=variable_id)
    except Variable.DoesNotExist:
        logger.warning(
            f"Cannot evaluate operations device. Variable with id {variable_id} does not exist."
        )
    else:
        if v.query_prev_value(**kwargs):
            logger.debug(f"prev value {v.prev_value} {kwargs}")
            if query_type == "timestamp":
                return v.timestamp_old
            elif query_type == "value":
                return v.prev_value
            else:
                logger.warning(f"Operation query type unknown : {query_type}")
    return None


class OperationsDataSource(models.Model):
    datasource = models.OneToOneField(DataSource, on_delete=models.CASCADE)
    inst = simpleeval.SimpleEval()
    inst.functions["variable"] = get_variable_value
    parsed_devices = {}
    evaluated_devices = {}
    parsed_variables = []
    evaluated_variables = []
    time_max = 0

    def parse_device(self, device):
        """
        parse the master operation if not done for a device
        """
        try:
            if not hasattr(device, "operationsdevice"):
                logger.warning(f"Cannot parse non operations device : {device}")
                return False
            if device.id not in self.parsed_devices:
                logger.debug(f"parsing : {device.operationsdevice.master_operation}")
                self.parsed_devices[device.id] = self.inst.parse(
                    str(device.operationsdevice.master_operation)
                )
        except Exception as e:
            logger.warning(
                f"{device} device - simple eval error for master operation {device.operationsdevice.master_operation} : {e}"
            )
            return False
        return True

    def eval_variable(self, variable):
        """
        will be called before the first read_data
        """
        try:
            if not hasattr(variable, "operationsdevice"):
                logger.warning(f"Cannot parse non operations variable : {variable}")
                return None
            if variable.id not in self.parsed_variables:
                if variable.operationsvariable.second_operation == "":
                    return self.evaluated_devices[variable.device.id]
                else:
                    my_names.update(
                        device_value=self.evaluated_devices[variable.device.id]
                    )
                    return self.inst.eval(
                        variable.operationsvariable.second_operation, names=my_names
                    )
        except Exception as e:
            logger.warning(
                f"{device} simple eval error for second operation {variable.operationsvariable.second_operation} : {e}"
            )
        return None

    def __str__(self):
        return f"OperationsDataSource"

    def last_value(self, **kwargs):
        read_time = None
        read_value = None
        logger.debug(kwargs)
        if "variable" in kwargs:
            variable = kwargs["variable"]
            logger.debug(variable.device.operationsdevice.get_variable_ids())
            time_min = kwargs["time_min"] if "time_min" in kwargs else 0
            time_max = kwargs["time_max"] if "time_max" in kwargs else time()
            result = self.read_multiple(
                variable_ids=[variable.id], time_min=time_min, time_max=time_max
            )
            if variable.id in result:
                read_time = result[variable.id][-1][0]
                read_value = result[variable.id][-1][1]
                logger.debug(
                    f"OperationsDataSource - read value from datasource - {variable} : {result[variable.id][-1]}"
                )
            else:
                logger.debug(
                    f"No value found for {variable} in range : {time_min} - {time_max}"
                )
                return None
        else:
            logger.warning(
                f"OperationsDataSource - read value from datasource - missing variable in kwargs"
            )
            return None
        return [read_time, read_value]

    def eval_device(
        self,
        device,
        time_min=0,
        time_max=time(),
        time_max_excluded=True,
        time_in_ms=False,
        **kwargs,
    ):
        # evaluate each device
        global global_time_min
        global global_time_max
        global global_time_max_excluded
        m_o = device.operationsdevice.master_operation
        parsed = self.parsed_devices[device.id]
        if time_in_ms:
            time_min = time_min / 1000
            time_max = time_max / 1000
        global_time_min = time_min
        global_time_max = time_max
        global_time_max_excluded = time_max_excluded
        try:
            result = self.inst.eval(m_o, previously_parsed=parsed)
        except TypeError:
            result = None
        return result

    def read_multiple(self, **kwargs):
        logger.debug(kwargs)
        variable_ids = kwargs.pop("variable_ids") if "variable_ids" in kwargs else []
        time_min = kwargs.pop("time_min") if "time_min" in kwargs else 0
        time_max = kwargs.pop("time_max") if "time_max" in kwargs else time()
        time_in_ms = kwargs.pop("time_in_ms") if "time_in_ms" in kwargs else True
        query_first_value = (
            kwargs.pop("query_first_value") if "query_first_value" in kwargs else False
        )
        time_max_excluded = kwargs.get("time_max_excluded", False)
        variable_ids = self.datasource.datasource_check(
            variable_ids, items_as_id=True, ids_model=Variable
        )

        # ouput = {"timestamp": time() * 1000, "date_saved_max": time() * 1000}
        output = {}
        logger.debug(f"Operations read for {variable_ids} {time_in_ms}")

        # parse master and sub operation
        self.parsed_devices = {}
        for v_id in variable_ids:
            self.parse_device(Variable.objects.get(id=v_id).device)

        logger.debug(self.parsed_devices)

        # iterate over time
        self.evaluated_devices = []
        for d_id in self.parsed_devices:
            logger.debug(d_id)
            device = Device.objects.get(id=d_id)
            if device.operationsdevice.synchronisation == 0:
                # calendar
                logger.debug("calendar")
                self.period_item = Period(
                    device.operationsdevice.start_from,
                    device.operationsdevice.period_factor,
                    device.operationsdevice.period_choices[
                        device.operationsdevice.period
                    ][1],
                )
                d1 = datetime.fromtimestamp(
                    max(time_min, device.operationsdevice.start_from.timestamp())
                )
                d2 = datetime.fromtimestamp(time_max)

                logger.debug(
                    f"{d1} {d2} {self.period_item.period_diff_quantity(d1, d2)}"
                )

                if self.period_item.period_diff_quantity(d1, d2) is None:
                    logger.debug(
                        "No period in date interval : %s (%s %s)"
                        % (self.period_item, d1, d2)
                    )
                    continue

                td = self.period_item.add_timedelta()

                d = self.period_item.get_valid_range(d1, d2)
                if d is None:
                    logger.debug(
                        "No time range found [%s to %s] %s" % (d1, d2, self.period_item)
                    )
                    continue
                [d1, d2] = d

                if self.period_item.period_diff_quantity(d1, d2) is None:
                    logger.debug(
                        "No period in new date interval : %s (%s %s)"
                        % (self.period_item, d1, d2)
                    )
                    continue

                logger.debug("Valid range : %s - %s" % (d1, d2))

                while d2 >= d1 + td and d1 + td <= now():
                    logger.debug("add for %s - %s" % (d1, d1 + td))
                    td1 = d1.timestamp()
                    evaluated_device = self.eval_device(
                        device, time_min=d1.timestamp(), time_max=(d1 + td).timestamp()
                    )
                    logger.debug([d1.timestamp(), evaluated_device])

                    # eval
                    for v_id in variable_ids:
                        if Variable.objects.get(id=v_id).device == device:
                            if v_id not in output:
                                output[v_id] = []
                            if evaluated_device is not None:
                                timestamp = (
                                    d1.timestamp() * 1000
                                    if time_in_ms
                                    else d1.timestamp()
                                )
                                logger.debug(f"append {timestamp} {evaluated_device}")
                                output[v_id].append([timestamp, evaluated_device])
                    d1 = d1 + td
            if device.operationsdevice.synchronisation == 1:
                # variable trigger
                logger.debug("trigger")
                trigger_variable = device.operationsdevice.trigger
                if (
                    trigger_variable.datasource.get_related_datasource().__class__.__name__
                    == self.__class__.__name__
                ):
                    logger.warning(
                        f"Trigger variable of the operations device {device} cannot be a variable using {self.__class__.__name__} as datasource"
                    )
                    continue
                logger.debug(
                    f"reading values of trigger variable {trigger_variable} in {time_min}, {time_max} as time_in_ms {False} without first value"
                )
                trigger_data = Variable.objects.read_multiple(
                    variable_ids=[trigger_variable.id],
                    time_min=time_min,
                    time_max=time_max,
                    time_in_ms=False,
                    query_first_value=False,
                )
                if trigger_variable.id in trigger_data:
                    logger.debug(len(trigger_data[trigger_variable.id]))
                    logger.debug(trigger_data[trigger_variable.id])
                    for i in range(len(trigger_data[trigger_variable.id])):
                        t_from = trigger_data[trigger_variable.id][i][0]
                        if i + 1 < len(trigger_data[trigger_variable.id]):
                            t_to = trigger_data[trigger_variable.id][i + 1][0]
                        else:
                            t_to = time_max
                        logger.debug(f"{i} {t_from} {t_to}")
                        if t_from == t_to:
                            # Do not exclude time_max
                            tmp_time_max_excluded = time_max_excluded
                        else:
                            tmp_time_max_excluded = True
                        evaluated_device = self.eval_device(
                            device,
                            time_min=t_from,
                            time_max=t_to,
                            time_max_excluded=tmp_time_max_excluded,
                        )
                        logger.debug([t_from, evaluated_device])

                        # eval
                        for v_id in variable_ids:
                            if Variable.objects.get(id=v_id).device == device:
                                if v_id not in output:
                                    output[v_id] = []
                                if evaluated_device is not None:
                                    timestamp = t_from * 1000 if time_in_ms else t_from
                                    logger.debug(
                                        f"append {timestamp} {evaluated_device} {time_in_ms}"
                                    )
                                    output[v_id].append([timestamp, evaluated_device])
                else:
                    logger.debug(
                        f"Trigger variable {trigger_variable} has no data in {time_min} - {time_max} range"
                    )

        return output

    def write_multiple(self, **kwargs):
        pass

    def get_first_element_timestamp(self, **kwargs):
        pass

    def get_last_element_timestamp(self, **kwargs):
        pass


class OperationsVariable(models.Model):
    operations_variable = models.OneToOneField(Variable, on_delete=models.CASCADE)
    second_operation = models.CharField(
        default="",
        max_length=400,
        blank=True,
        help_text=mark_safe(
            "Examples in the <a href='https://github.com/clavay/PyScada-Operations'>readme</a>."
        ),
    )

    @property
    def parent_variable(self):
        try:
            return self.operations_variable
        except:
            return None

    def __str__(self):
        return self.parent_variable.name

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        if not OperationsDataSource.objects.all().count():
            logger.warning("OperationsDataSource is missing !")
            return
        if (
            self.operations_variable.datasource
            != OperationsDataSource.objects.first().datasource
        ):
            # Set the datasource
            self.operations_variable.datasource = (
                OperationsDataSource.objects.first().datasource
            )
            self.operations_variable.save(update_fields=["datasource"])
            logger.info(
                f"Update {self.operations_variable} datasource to OperationsDataSource"
            )


def validate_nonzero(value):
    if value == 0:
        raise ValidationError(
            _("Quantity %(value)s is not allowed"),
            params={"value": value},
        )


def start_from_default():
    return make_aware(
        datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
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


class OperationsDevice(models.Model):
    operations_device = models.OneToOneField(Device, on_delete=models.CASCADE)
    master_operation = models.CharField(
        default="",
        max_length=400,
        help_text=mark_safe(
            "Examples in the <a href='https://github.com/clavay/PyScada-Operations'>readme</a>."
        ),
    )
    synchronisation_choices = (
        (0, "calendar"),
        (1, "trigger"),
    )
    synchronisation = models.PositiveSmallIntegerField(
        default=0, choices=synchronisation_choices
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
    period = models.SmallIntegerField(choices=period_choices, blank=True, null=True)
    period_factor = models.PositiveSmallIntegerField(
        default=1,
        validators=[validate_nonzero],
        help_text="Example: set to 2 and choose minute to have a 2 minutes period",
    )
    trigger = models.ForeignKey(
        Variable, on_delete=models.CASCADE, blank=True, null=True
    )

    class FormSet(BaseInlineFormSet):
        def add_fields(self, form, index):
            super().add_fields(form, index)
            form.fields["synchronisation"].widget.attrs = {
                # hidden by default
                "--hideshow-fields": "start_from, period, period_factor, trigger",
                # calendar
                "--show-on-0": "start_from, period, period_factor",
                # trigger
                "--show-on-1": "trigger",
            }

    def clean(self):
        super().clean()
        if self.synchronisation == 1 and self.trigger is None:
            raise ValidationError("Select a trigger variable.")
        if self.synchronisation == 0 and self.period is None:
            raise ValidationError("Enter a period.")

    def get_variable_ids(self):
        variable_ids = []
        first = True
        for i in self.master_operation.split("variable("):
            if first:
                # skip firt item
                first = False
                continue
            if ")" not in i:
                logger.warning(
                    f"{self} master expression is malformed. variable( parenthesis not closed."
                )
                break
            id = i.split(")")[0]
            try:
                if float(id) != int(id):
                    raise ValueError
                else:
                    variable_ids.append(int(id))
            except ValueError:
                logger.warning(
                    f"variable id in master operation should be an integer, it is : {id}"
                )
        return variable_ids

    def parent_device(self):
        try:
            return self.operations_device
        except:
            return None

    def __str__(self):
        return self.operations_device.short_name


class AggregatedVariable(models.Model):
    aggregated_variable = models.OneToOneField(Variable, on_delete=models.CASCADE)
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
            return self.aggregated_variable.device.aggregateddevice.variable
        except Exception as e:
            logger.warning(e)
            return None

    @property
    def parent_variable(self):
        try:
            return self.aggregated_variable
        except:
            return None

    def __str__(self):
        if self.parent_variable is not None:
            return self.parent_variable.name
        return "EmptyAggregatedVariable"


class AggregatedDevice(models.Model):
    aggregated_device = models.OneToOneField(Device, on_delete=models.CASCADE)
    variable = models.ForeignKey(
        Variable, on_delete=models.CASCADE, blank=True, null=True
    )

    def clean(self):
        super().clean()
        if self.variable is None:
            raise ValidationError("Select a variable to aggregate.")

    def parent_device(self):
        try:
            return self.aggregated_device
        except:
            return None

    def __str__(self):
        if self.parent_device() is not None:
            return self.parent_device().short_name
        return "EmptyAggregatedDevice"


class Period(object):
    def __init__(self, start_from, period_factor, period_str):
        self.start_from = start_from
        self.period_factor = period_factor
        self.period_str = period_str
        # period_str = self.period.period_choices[self.period.period][1]

    def __str__(self):
        return f"{self.start_from} {self.period_factor} {self.period_str}"

    def get_valid_range(self, d1, d2):
        if is_naive(d1):
            d1 = make_aware(d1)
        if is_naive(d2):
            d2 = make_aware(d2)
        if d2 <= d1:
            logger.warning("Use get_valid_range with d_start > d_end")
            return None
        if self.start_from == d1:
            logger.info("strart from is d1")
            d_start = 0
        else:
            logger.info(f"{self.start_from} {d1} {self.period_str}")
            logger.info(d1 - self.start_from)
            logger.info((d1 - self.start_from).total_seconds())
            # (d2 - d1).total_seconds() / 60 / 60
            d_start = self.period_diff_quantity(self.start_from, d1)
            logger.info(f"strart from is not d1, it is {d_start}")
            if d_start is not None:
                if d_start != int(d_start):
                    d_start = int(d_start) + 1
                else:
                    d_start = int(d_start)
            else:
                logger.debug("d_start - start_from < period_factor*period")
                return None
        d_end = self.period_diff_quantity(self.start_from, d2)
        logger.info(d_end)
        if d_end is not None:
            d_end = int(d_end)
        else:
            logger.debug("d_end - start_from < period_factor*period")
            return None
        if d_end <= d_start:
            logger.debug("d_end - d_start < period_factor*period")
            return None

        td = self.add_timedelta()

        d_start = d_start / self.period_factor
        if d_start != int(d_start):
            d_start = int(d_start) + 1
        else:
            d_start = int(d_start)

        d_end = d_end / self.period_factor
        if d_end != int(d_end):
            d_end = int(d_end) + 1
        else:
            d_end = int(d_end)

        dd_start = d_start * td + self.start_from
        dd_end = d_end * td + self.start_from

        logger.info(f"{d_start} {d_end} {dd_start} {dd_end}")

        if dd_end > d2:
            logger.debug("%s > %s" % (dd_end, d2))
            dd_end -= self.add_timedelta(self._period_diff_quantity(d2, dd_end))
            logger.debug("dd_end : %s" % dd_end)

        return [dd_start, dd_end]

    def add_timedelta(self, delta=None):
        if delta is None:
            delta = self.period_factor
        td = None
        if self.period_str == "year":
            td = monthdelta(12) * delta
        elif self.period_str == "month":
            td = monthdelta(delta)
        elif self.period_str == "week":
            td = timedelta(weeks=delta)
        elif self.period_str == "day":
            td = timedelta(days=delta)
        elif self.period_str == "hour":
            td = timedelta(hours=delta)
        elif self.period_str == "minute":
            td = timedelta(minutes=delta)
        elif self.period_str == "second":
            td = timedelta(seconds=delta)
        return td

    def _period_diff_quantity(self, d1, d2):
        if self.period_str == "year":
            res = self.years_diff_quantity(d1, d2)
        elif self.period_str == "month":
            res = self.months_diff_quantity(d1, d2)
        elif self.period_str == "week":
            res = self.weeks_diff_quantity(d1, d2)
        elif self.period_str == "day":
            res = self.days_diff_quantity(d1, d2)
        elif self.period_str == "hour":
            res = self.hours_diff_quantity(d1, d2)
        elif self.period_str == "minute":
            res = self.minutes_diff_quantity(d1, d2)
        elif self.period_str == "second":
            res = self.seconds_diff_quantity(d1, d2)
        return res

    def period_diff_quantity(self, d1, d2):
        res = self._period_diff_quantity(d1, d2)
        if res >= self.period_factor:
            return res
        else:
            return None

    def years_diff_quantity(self, d1, d2):
        return relativedelta.relativedelta(d2, d1).years

    def months_diff_quantity(self, d1, d2):
        return (
            relativedelta.relativedelta(d2, d1).months
            + self.years_diff_quantity(d1, d2) * 12
        )

    def weeks_diff_quantity(self, d1, d2):
        return self.days_diff_quantity(d1, d2) / 7

    def days_diff_quantity(self, d1, d2):
        diff = (d2 - d1).total_seconds() / 60 / 60 / 24
        # logger.debug("Days: " + str(diff))
        return diff

    def hours_diff_quantity(self, d1, d2):
        diff = (d2 - d1).total_seconds() / 60 / 60
        # logger.debug("Hours: " + str(diff))
        return diff

    def minutes_diff_quantity(self, d1, d2):
        diff = (d2 - d1).total_seconds() / 60
        # logger.debug("Minutes: " + str(diff))
        return diff

    def seconds_diff_quantity(self, d1, d2):
        diff = (d2 - d1).total_seconds()
        # logger.debug("Seconds: " + str(diff))
        return diff

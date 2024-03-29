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
from . import PROTOCOL_ID

from time import time
from datetime import datetime, timedelta, date
from dateutil import relativedelta
from monthdelta import monthdelta
import simpleeval
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


def validate_nonzero(value):
    if value == 0:
        raise ValidationError(
            _("Quantity %(value)s is not allowed"),
            params={"value": value},
        )


def start_from_default():
    return make_aware(datetime.combine(date.today(), datetime.min.time()))


class OperationsDataSource(models.Model):
    datasource = models.OneToOneField(DataSource, on_delete=models.CASCADE)
    inst = simpleeval.SimpleEval()
    inst.functions["variable"] = get_variable_value
    parsed_devices = {}
    evaluated_devices = {}
    parsed_variables = []
    evaluated_variables = []
    time_max = 0
    time_max_tmp = {}

    def parse_device(self, device):
        """
        parse the master operation if not done for a device
        """
        try:
            if not hasattr(device, "operationsdevice"):
                logger.warning(f"Cannot parse non operations device : {device}")
                return False
            if device.id not in self.parsed_devices:
                self.parsed_devices[device.id] = self.inst.parse(
                    str(device.operationsdevice.master_operation)
                )
                logger.debug(f"parsing {device} : {device.operationsdevice.master_operation} - {self.parsed_devices}")
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
        if "variable" in kwargs:
            variable = kwargs.pop("variable")
            logger.debug(variable.device.operationsdevice.get_variable_ids())
            time_min = kwargs.pop("time_min") if "time_min" in kwargs else 0
            time_max = kwargs.pop("time_max") if "time_max" in kwargs else time()
            time_in_ms = kwargs.pop("time_in_ms") if "time_in_ms" in kwargs else True
            result = self.query_data(
                variable_ids=[variable.id],
                time_min=time_min,
                time_max=time_max,
                time_in_ms=time_in_ms,
                order="desc",
                quantity=1,
                **kwargs,
            )
            if variable.id in result and len(result[variable.id]):
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
        if device.id not in self.parsed_devices:
            logger.debug(f"device {device} not parsed")
            return None
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
        return self.query_data(**kwargs)

    def query_data(self, quantity=None, order="asc", **kwargs):
        t_start = time()
        output = {}
        if order not in ["asc", "desc"]:
            logger.warning(f"Wrong order to query data : {order}")
            return output
        if quantity is not None and type(quantity) != int:
            logger.warning(f"Wrong quantity to query data : {quantity}")
            return output
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
        logger.debug(f"Operations read for {variable_ids} {time_in_ms}")

        # parse master and sub operation
        #self.parsed_devices = {}
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

                period_diff_quantity = self.period_item.period_diff_quantity(d1, d2)
                logger.debug(f"Valid range : {d1} - {d2} - {period_diff_quantity}")

                i = j = 0
                stop = False

                while not stop:
                    if order == "asc":
                        dx = d1 + i * td
                        logger.debug("asc add for %s - %s" % (dx, dx + td))
                        evaluated_device = self.eval_device(
                            device,
                            time_min=(dx).timestamp(),
                            time_max=(dx + td).timestamp(),
                        )
                        if dx + td >= min(d2, now()):
                            logger.debug(
                                f"will stop iterating {d1} {td} {i} {d2} {now()} {d1 + (i+1)*td} {min(d2, now())}"
                            )
                            stop = True
                    elif order == "desc":
                        dx = d2 - i * td
                        logger.debug("desc add for %s - %s" % (dx, dx + td))
                        evaluated_device = self.eval_device(
                            device,
                            time_min=(dx).timestamp(),
                            time_max=(dx + td).timestamp(),
                        )
                        if dx <= min(d1, now()):
                            logger.debug(
                                f"will stop iterating {d1} {td} {i} {d2} {now()} {d1 + (i+1)*td} {min(d2, now())}"
                            )
                            stop = True
                    #                            device, time_min=(d1 + (q)).timestamp(), time_max=(d1 + td).timestamp()
                    logger.debug([dx.timestamp(), evaluated_device])

                    # eval
                    for v_id in variable_ids:
                        var = Variable.objects.get(id=v_id)
                        if var.device == device:
                            self.time_max_tmp[v_id] = time_max
                            if v_id not in output:
                                output[v_id] = []
                            if evaluated_device is not None:
                                timestamp = (
                                    dx.timestamp() * 1000
                                    if time_in_ms
                                    else dx.timestamp()
                                )
                                logger.debug(f"append {timestamp} {evaluated_device}")
                                output[v_id].append([timestamp, evaluated_device])
                                self.time_max_tmp[v_id] = min(self.time_max_tmp[v_id], t_from)
                    # d1 = d1 + td
                    if evaluated_device is not None:
                        j += 1
                    if quantity is not None and quantity <= j:
                        break
                    i += 1
                    if "timeout" in kwargs and kwargs["timeout"] < time() - t_start:
                        stop = True
                        logger.info(
                            f"Timeout of {kwargs['timeout']} seconds reached in query data for OperationsDataSource."
                        )
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
                    f"reading values of trigger variable {trigger_variable} in {time_min}, {time_max} as time_in_ms False without first value"
                )
                trigger_data = Variable.objects.read_multiple(
                    variable_ids=[trigger_variable.id],
                    time_min=time_min,
                    time_max=time_max,
                    time_in_ms=True,
                    query_first_value=False,
                )
                if trigger_variable.id in trigger_data:
                    logger.debug(len(trigger_data[trigger_variable.id]))
#                    logger.debug(trigger_data[trigger_variable.id])
                    data_length = len(trigger_data[trigger_variable.id])
                    j = 0
                    for i in range(data_length):
                        if order == "asc":
                            t_from = trigger_data[trigger_variable.id][i][0] / 1000
                            if i + 1 < data_length:
                                t_to = trigger_data[trigger_variable.id][i + 1][0] / 1000
                            else:
                                t_to = time_max
                        elif order == "desc":
                            t_from = trigger_data[trigger_variable.id][
                                data_length - i - 1
                            ][0] / 1000
                            if i > 0:
                                t_to = trigger_data[trigger_variable.id][
                                    data_length - i
                                ][0] / 1000
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
                            var = Variable.objects.get(id=v_id)
                            if var.device == device:
                                if v_id not in self.time_max_tmp:
                                    self.time_max_tmp[v_id] = time_max
                                if v_id not in output:
                                    output[v_id] = []
                                if evaluated_device is not None:
                                    timestamp = t_from * 1000 if time_in_ms else t_from
                                    output[v_id].append([timestamp, evaluated_device])
                                    self.time_max_tmp[v_id] = min(self.time_max_tmp[v_id], t_from, time_max)
                        if evaluated_device is not None:
                            j += 1
                        if quantity is not None and quantity <= j:
                            break
                        if "timeout" in kwargs and kwargs["timeout"] < time() - t_start:
                            logger.info(
                                f"Timeout of {kwargs['timeout']} seconds reached in query data for OperationsDataSource."
                            )
                            break
                else:
                    logger.debug(
                        f"Trigger variable {trigger_variable} has no data in {time_min} - {time_max} range"
                    )
            for v_id in variable_ids:
                var = Variable.objects.get(id=v_id)
                if var.device == device:
                    if query_first_value:
                        tm = self.time_max_tmp[v_id] if v_id in self.time_max_tmp else time()
                        last_value = self.last_value(variable=var, time_max=tm)
                        if last_value is not None:
                            if v_id not in output:
                                output[v_id] = []
                            output[v_id].insert(0, last_value)
        return output

    def write_multiple(self, **kwargs):
        pass

    def get_first_element_timestamp(self, **kwargs):
        pass

    def get_last_element_timestamp(self, **kwargs):
        pass


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
        elif self.synchronisation == 1 and self.trigger.device.protocol.id == PROTOCOL_ID:
            raise ValidationError("Select a trigger variable which is not from an operation device.")
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
            logger.debug("strart from is d1")
            d_start = 0
        else:
            logger.debug(f"{self.start_from} {d1} {self.period_str}")
            logger.debug(d1 - self.start_from)
            logger.debug((d1 - self.start_from).total_seconds())
            # (d2 - d1).total_seconds() / 60 / 60
            d_start = self.period_diff_quantity(self.start_from, d1)
            logger.debug(f"strart from is not d1, it is {d_start}")
            if d_start is not None:
                if d_start != int(d_start):
                    d_start = int(d_start) + 1
                else:
                    d_start = int(d_start)
            else:
                logger.debug("d_start - start_from < period_factor*period")
                return None
        d_end = self.period_diff_quantity(self.start_from, d2)
        logger.debug(d_end)
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

        logger.debug(f"{d_start} {d_end} {dd_start} {dd_end}")

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

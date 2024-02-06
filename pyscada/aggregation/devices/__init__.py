# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from .. import PROTOCOL_ID
from pyscada.operations.models import Period
from pyscada.device import GenericHandlerDevice
from pyscada.models import Variable


from django.conf import settings
from django.utils.timezone import now, make_aware, is_naive

try:
    import numpy as np

    driver_ok = True
except ImportError:
    driver_ok = False

from time import time

import logging

logger = logging.getLogger(__name__)


class GenericDevice(GenericHandlerDevice):
    def __init__(self, pyscada_device, variables):
        super().__init__(pyscada_device, variables)
        self._protocol = PROTOCOL_ID
        self.driver_ok = driver_ok

    def connect(self):
        """
        establish a connection to the Instrument
        """
        super().connect()
        return True

    def read_data_all(self, variables_dict):
        output = []

        if self.before_read():
            for item in variables_dict.values():
                output += self.read_data(item)
        self.after_read()
        return output

    def read_data(self, variable_instance):
        """
        read values using aggregate function and selected period
        """
        return self.check_to_now(variable_instance)

    def check_to_now(
        self,
        variable_instance,
        force_write=False,
        add_partial_info=False,
        ignore_last_check=False,
    ):
        last_check = variable_instance.aggregationvariable.last_check
        start_from = variable_instance.device.aggregationdevice.start_from
        if last_check is not None and not ignore_last_check:
            return self.check_period(
                variable_instance, last_check, now(), force_write, add_partial_info
            )
        else:
            return self.check_period(
                variable_instance, start_from, now(), force_write, add_partial_info
            )

    def check_period(
        self, variable_instance, d1, d2, force_write=False, add_partial_info=False
    ):
        # erase cached values from other Calculated Variables with same store value
        agg_var = variable_instance.aggregationvariable
        variable_instance.update_values([], [], erase_cache=True)
        logger.debug(f"Check period of {variable_instance} [{d1} to {d2}]")
        agg_var.state = f"Checking [{d1} to {d2}]"
        agg_var.state = agg_var.state[0:100]
        agg_var.save(update_fields=["state"])

        # create period object
        self.period_item = Period(
            variable_instance.device.aggregationdevice.start_from,
            variable_instance.device.aggregationdevice.period_factor,
            variable_instance.device.aggregationdevice.period_choices[
                variable_instance.device.aggregationdevice.period
            ][1],
        )

        if is_naive(d1):
            d1 = make_aware(d1)
        if is_naive(d2):
            d2 = make_aware(d2)
        output = []

        if self.period_item.period_diff_quantity(d1, d2) is None:
            logger.debug(
                f"No period in date interval : {variable_instance} [{d1} to {d2}]"
            )
            agg_var.state = f"[{d1} to {d2}] < {variable_instance.device.aggregationdevice.period_factor} {variable_instance.device.aggregationdevice.period_choices[variable_instance.device.aggregationdevice.period][1]}"
            agg_var.state = agg_var.state[0:100]
            agg_var.save(update_fields=["state"])
            return output

        td = self.period_item.add_timedelta()

        d = self.period_item.get_valid_range(d1, d2)
        if d is None:
            agg_var.state = f"No time range found [{d1} to {d2}] {variable_instance}"
            agg_var.state = agg_var.state[0:100]
            agg_var.save(update_fields=["state"])
            return output
        [d1, d2] = d

        if self.period_item.period_diff_quantity(d1, d2) is None:
            logger.debug(
                f"No period in new date interval : {variable_instance} [{d1} to {d2}]"
            )
            agg_var.state = f"[{d1} to {d2}] < {variable_instance.device.aggregationdevice.period_factor} {variable_instance.device.aggregationdevice.period_choices[variable_instance.device.aggregationdevice.period][1]}"
            agg_var.state = agg_var.state[0:100]
            agg_var.save(update_fields=["state"])
            return output

        logger.debug(f"Valid range : [{d1} to {d2}] for {variable_instance}")
        to_store = False
        while (
            d2.timestamp()
            - variable_instance.device.aggregationdevice.calculation_wait_offset
            >= (d1 + td).timestamp()
            and d1 + td <= now()
        ):
            logger.debug(f"add [{d1} to {d1 + td}] for {variable_instance}")
            td1 = d1.timestamp()
            try:
                v_stored = Variable.objects.read_multiple(
                    variable_ids=[variable_instance.id],
                    time_min=td1,
                    time_max=td1 + 1,
                )
            except AttributeError:
                v_stored = {}
            if not force_write and len(v_stored) and variable_instance.id in v_stored:
                logger.debug(f"Value already exist for {agg_var} in {d1} - {d1 + td}")
                pass
            else:
                calc_value = self.get_value(variable_instance, d1, d1 + td)
                if calc_value is not None and variable_instance.update_values(
                    [calc_value],
                    [td1 + variable_instance.device.aggregationdevice.timestamp_offset],
                    erase_cache=False,
                ):
                    to_store = True
            d1 = d1 + td

        variable_instance.date_saved = now()
        output.append(variable_instance)

        if len(output):
            agg_var.last_check = output[-1].date_saved  # + td
        else:
            logger.debug("Nothing to add")
            agg_var.last_check = min(d1, d2, now())

        # Add partial last value when there is data but the period is not elapsed
        # do not use this data in last check to recalculate it again till the period is elapsed
        if add_partial_info:
            calc_value = self.get_value(variable_instance, d2 - td, d2)
            td2 = (d2 - td).timestamp()
            if calc_value is not None:
                logger.debug(
                    f"adding partial last value in [{d2 - td} to {d2}] for {variable_instance}"
                )
                variable_instance.update_values(
                    [calc_value],
                    [td2 + variable_instance.device.aggregationdevice.timestamp_offset],
                    erase_cache=False,
                )

        # Save recorded data elements to DB
        if len(output):
            m = "Adding : "
            for c in output:
                m += str(c) + " " + str(c.date_saved) + " - "
            logger.debug(m)
            Variable.objects.write_multiple(
                items=output, batch_size=100, ignore_conflicts=True
            )

        agg_var.state = f"Checked [{d1} to {d2}]"
        agg_var.state = agg_var.state[0:100]
        agg_var.save(update_fields=["last_check", "state"])

        return output

    def get_value(self, variable_instance, d1, d2):
        agg_var = variable_instance.aggregationvariable
        logger.debug(
            f"getting value for {variable_instance} in {d1} {d1.timestamp()} ({variable_instance.device.aggregationdevice.calculation_start_offset}) {d2} {d2.timestamp()} ({variable_instance.device.aggregationdevice.calculation_end_offset})"
        )
        try:
            tmp = Variable.objects.read_multiple(
                variable_ids=[agg_var.variable.id],
                time_min=d1.timestamp()
                + variable_instance.device.aggregationdevice.calculation_start_offset,
                time_max=d2.timestamp()
                + variable_instance.device.aggregationdevice.calculation_end_offset,
                time_in_ms=True,
                time_max_excluded=True,
            )
            tmpCount = (
                len(tmp[agg_var.variable.id]) if agg_var.variable.id in tmp else 0
            )
            logger.debug(f"get values for {variable_instance} : {tmpCount}")
        except AttributeError:
            tmp = {}
        values = []
        if agg_var.variable.id in tmp:
            for v in tmp[agg_var.variable.id]:
                values.append(v[1])
            type_str = variable_instance.device.aggregationdevice.type_choices[
                variable_instance.device.aggregationdevice.type
            ][1]
            if type_str == "min":
                p = str(variable_instance.device.aggregationdevice.property)
                if p == "" or p is None or p == "None":
                    res = min(values)
                elif p.startswith("<"):
                    try:
                        p = float(p.split("<")[1])
                        res = min_pass(values, p, "gt")
                    except ValueError:
                        logger.warning(
                            f"Period field {variable_instance.device.aggregationdevice} property after < is not a float : {variable_instance.device.aggregationdevice.property}"
                        )
                        res = None
                else:
                    try:
                        p = float(p)
                        res = min_pass(values, p, "gte")
                    except ValueError:
                        logger.warning(
                            f"Period field {variable_instance.device.aggregationdevice} property is not a float : {variable_instance.device.aggregationdevice.property}"
                        )
                        res = None
            elif type_str == "max":
                p = str(variable_instance.device.aggregationdevice.property)
                if p == "" or p is None or p == "None":
                    res = max(values)
                elif p.startswith(">"):
                    try:
                        p = float(p.split(">")[1])
                        res = max_pass(values, p, "lt")
                    except ValueError:
                        logger.warning(
                            f"Period field {variable_instance.device.aggregationdevice} property after > is not a float : {variable_instance.device.aggregationdevice.property}"
                        )
                        res = None
                else:
                    try:
                        p = float(p)
                        res = max_pass(values, p, "lte")
                    except ValueError:
                        logger.warning(
                            f"Period field {variable_instance.device.aggregationdevice} property is not a float : {variable_instance.device.aggregationdevice.property}"
                        )
                        res = None
            elif type_str == "total":
                res = sum(values)
            elif type_str == "difference":
                res = values[-1] - values[0]
            elif type_str == "difference percent":
                if values[0] == 0:
                    res = None
                else:
                    res = (values[-1] - values[0]) / (values[0] * 100)
            elif type_str == "delta":
                res = 0
                v = None
                for i in values:
                    if v is not None and i - v > 0:
                        res += i - v
                    v = i
            elif type_str == "mean":
                res = np.mean(values)
            elif type_str == "first":
                res = values[0]
            elif type_str == "last":
                res = values[-1]
            elif type_str == "count":
                res = len(values)
            elif type_str == "count value":
                try:
                    p = float(variable_instance.device.aggregationdevice.property)
                    res = values.count(p)
                except ValueError:
                    logger.warning(
                        f"Period field {variable_instance.device.aggregationdevice} property is not a float"
                    )
                    res = None
            elif type_str == "range":
                res = max(values) - min(values)
            elif type_str == "step":
                res = 0
                j = None
                if len(values) > 1:
                    for i in values:
                        if j is not None:
                            res = min(res, abs(i - j))
                        j = i
                else:
                    res = None
            elif type_str == "change count":
                res = 0
                j = None
                if len(values) > 1:
                    for i in values:
                        if j is not None and j != i:
                            res += 1
                        j = i
                else:
                    res = None
            elif type_str == "distinct count":
                res = len(set(values))
            else:
                logger.warning("Periodic field type unknown")
                res = None

            logger.debug(
                f"read {res} for {agg_var.variable} in {d1} {d1.timestamp()} {d2} {d2.timestamp()}"
            )
            return res
        else:
            # logger.debug("No values for this period")
            return None

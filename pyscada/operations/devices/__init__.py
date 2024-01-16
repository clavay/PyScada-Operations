# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from .. import PROTOCOL_ID_2 as PROTOCOL_ID
from ..models import Period
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
        last_check = variable_instance.aggregatedvariable.last_check
        start_from = variable_instance.aggregatedvariable.period.start_from
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
        agg_var = variable_instance.aggregatedvariable
        variable_instance.update_values([], [], erase_cache=True)
        logger.debug("Check period of %s [%s - %s]" % (variable_instance, d1, d2))
        agg_var.state = "Checking [%s to %s]" % (d1, d2)
        agg_var.state = agg_var.state[0:100]
        agg_var.save(update_fields=["state"])

        # create period object
        self.period_item = Period(
            agg_var.period.start_from,
            agg_var.period.period_factor,
            agg_var.period.period_choices[agg_var.period.period][1],
        )

        if is_naive(d1):
            d1 = make_aware(d1)
        if is_naive(d2):
            d2 = make_aware(d2)
        output = []

        if self.period_item.period_diff_quantity(d1, d2) is None:
            logger.debug(
                "No period in date interval : %s (%s %s)" % (agg_var.period, d1, d2)
            )
            agg_var.state = "[%s to %s] < %s" % (
                d1,
                d2,
                str(agg_var.period.period_factor)
                + agg_var.period.period_choices[agg_var.period.period][1],
            )
            agg_var.state = agg_var.state[0:100]
            agg_var.save(update_fields=["state"])
            return output

        td = self.period_item.add_timedelta()

        d = self.period_item.get_valid_range(d1, d2)
        if d is None:
            agg_var.state = "No time range found [%s to %s] %s" % (
                d1,
                d2,
                agg_var.period,
            )
            agg_var.state = agg_var.state[0:100]
            agg_var.save(update_fields=["state"])
            return output
        [d1, d2] = d

        if self.period_item.period_diff_quantity(d1, d2) is None:
            logger.debug(
                "No period in new date interval : %s (%s %s)" % (agg_var.period, d1, d2)
            )
            agg_var.state = "[%s to %s] < %s" % (
                d1,
                d2,
                str(agg_var.period.period_factor)
                + agg_var.period.period_choices[agg_var.period.period][1],
            )
            agg_var.state = agg_var.state[0:100]
            agg_var.save(update_fields=["state"])
            return output

        logger.debug("Valid range : %s - %s" % (d1, d2))
        to_store = False
        while d2 >= d1 + td and d1 + td <= now():
            logger.debug("add for %s - %s" % (d1, d1 + td))
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
                logger.debug(
                    f"Value already exist for {self.aggregated_variable} in {d1} - {d1 + td}"
                )
                pass
            else:
                calc_value = self.get_value(variable_instance, d1, d1 + td)
                if calc_value is not None and variable_instance.update_values(
                    [calc_value], [td1], erase_cache=False
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

        # Add partial last value when then is data but the period is not elapsed
        # do not use this data in last check to recalculate it again till the period is elapsed
        calc_value = self.get_value(variable_instance, d2 - td, d2)
        td2 = (d2 - td).timestamp()
        if add_partial_info and calc_value is not None:
            logger.debug(f"adding partial last value for {d2 - td} {d2}")
            variable_instance.update_values([calc_value], [td2], erase_cache=False)

        # Save recorded data elements to DB
        if len(output):
            m = "Adding : "
            for c in output:
                m += str(c) + " " + str(c.date_saved) + " - "
            logger.debug(m)
            Variable.objects.write_multiple(
                items=output, batch_size=100, ignore_conflicts=True
            )

        agg_var.state = "Checked [%s to %s]" % (d1, d2)
        agg_var.state = agg_var.state[0:100]
        agg_var.save(update_fields=["last_check", "state"])

        return output

    def get_value(self, variable_instance, d1, d2):
        agg_var = variable_instance.aggregatedvariable
        main_variable = agg_var.main_variable
        logger.debug(
            f"getting value for {main_variable} in {d1} {d1.timestamp()} {d2} {d2.timestamp()}"
        )
        try:
            tmp = Variable.objects.read_multiple(
                variable_ids=[main_variable.id],
                time_min=d1.timestamp(),
                time_max=d2.timestamp(),
                time_in_ms=True,
                time_max_excluded=True,
            )
            logger.debug(f"get values : {tmp}")
        except AttributeError:
            tmp = {}
        values = []
        if main_variable.id in tmp:
            for v in tmp[main_variable.id]:
                values.append(v[1])
            type_str = agg_var.period.type_choices[agg_var.period.type][1]
            if type_str == "min":
                p = str(agg_var.period.property)
                if p == "" or p is None or p == "None":
                    res = min(values)
                elif p.startswith("<"):
                    try:
                        p = float(p.split("<")[1])
                        res = min_pass(values, p, "gt")
                    except ValueError:
                        logger.warning(
                            "Period field %s property after < is not a float : %s"
                            % (agg_var.period, agg_var.period.property)
                        )
                        res = None
                else:
                    try:
                        p = float(p)
                        res = min_pass(values, p, "gte")
                    except ValueError:
                        logger.warning(
                            "Period field %s property is not a float : %s"
                            % (agg_var.period, agg_var.period.property)
                        )
                        res = None
            elif type_str == "max":
                p = str(agg_var.period.property)
                if p == "" or p is None or p == "None":
                    res = max(values)
                elif p.startswith(">"):
                    try:
                        p = float(p.split(">")[1])
                        res = max_pass(values, p, "lt")
                    except ValueError:
                        logger.warning(
                            "Period field %s property after > is not a float : %s"
                            % (agg_var.period, agg_var.period.property)
                        )
                        res = None
                else:
                    try:
                        p = float(p)
                        res = max_pass(values, p, "lte")
                    except ValueError:
                        logger.warning(
                            "Period field %s property is not a float : %s"
                            % (agg_var.period, agg_var.period.property)
                        )
                        res = None
            elif type_str == "total":
                res = sum(values)
            elif type_str == "difference":
                res = values[-1] - values[0]
            elif type_str == "difference percent":
                res = (values[-1] - values[0]) / min(values)
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
                    p = float(agg_var.period.property)
                    res = values.count(p)
                except ValueError:
                    logger.warning(
                        "Period field %s property is not a float" % agg_var.period
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
                f"read {res} for {main_variable} in {d1} {d1.timestamp()} {d2} {d2.timestamp()}"
            )
            return res
        else:
            # logger.debug("No values for this period")
            return None

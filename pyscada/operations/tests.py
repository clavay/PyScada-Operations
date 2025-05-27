"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase
from django.apps import apps
from pyscada.models import Variable, Device, Unit
from pyscada.operations.models import OperationsVariable, OperationsDevice
from datetime import datetime
from pytz import UTC

import logging

logger = logging.getLogger(__name__)


class ReadOperationVariableTest(TestCase):
    def setUp(self):
        apps.get_app_config("pyscada").pyscada_app_init()
        apps.get_app_config("operations").pyscada_app_init()

        d, created = Device.objects.get_or_create(
            short_name="dev", description="dev", protocol_id=1
        )
        unit, created = Unit.objects.get_or_create(
            unit="unit", description="unit", udunit="unit"
        )
        self.v, created = Variable.objects.get_or_create(
            name="var", description="var", device=d, unit=unit
        )
        self.v.update_values([1, 10, 100, 1000], [0, 1, 2, 3])
        Variable.objects.write_multiple(
            items=[
                self.v,
            ],
            date_saved=datetime.fromtimestamp(5, UTC),
        )

        do, created = Device.objects.get_or_create(
            short_name="devOp", description="dev", protocol_id=18
        )
        doo = OperationsDevice.objects.get_or_create(
            operations_device=do,
            master_operation=f"2*variable({self.v.id})",
            synchronisation=1,
            trigger=self.v,
        )
        self.vo, created = Variable.objects.get_or_create(
            name="varOp", description="var", device=do, unit=unit
        )
        voo, created = OperationsVariable.objects.get_or_create(
            operations_variable=self.vo, second_operation="v"
        )

        do2, created = Device.objects.get_or_create(
            short_name="devOp2", description="dev", protocol_id=18
        )
        doo2 = OperationsDevice.objects.get_or_create(
            operations_device=do2,
            master_operation=f"2*variable({self.v.id})",
            synchronisation=0,
            start_from=datetime.fromtimestamp(0, UTC),
            period=0,
            period_factor=1,
        )
        self.vo2, created = Variable.objects.get_or_create(
            name="varOp2", description="var", device=do2, unit=unit
        )
        voo2, created = OperationsVariable.objects.get_or_create(
            operations_variable=self.vo2, second_operation="v"
        )

    def test_variable_read_on_trigger(self):
        """Variable read multiple test"""
        v = self.vo
        self.assertEqual(v.query_prev_value(), True)

        logger.debug("test_variable_read_on_trigger 1")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old,
            time_in_ms=False,
            query_first_value=True,
            time_max_excluded=True,
        )
        self.assertEqual(result, {"timestamp": 2.0, v.id: [[2.0, 200.0]]})

        logger.debug("test_variable_read_on_trigger 2")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old,
            time_in_ms=False,
            query_first_value=False,
            time_max_excluded=True,
        )
        self.assertEqual(result, {"timestamp": 0})

        logger.debug("test_variable_read_on_trigger 3")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old,
            time_in_ms=False,
            query_first_value=True,
            time_max_excluded=False,
        )
        self.assertEqual(
            result, {"timestamp": 3.0, v.id: [[2.0, 200.0], [3.0, 2000.0]]}
        )

        logger.debug("test_variable_read_on_trigger 4")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old,
            time_in_ms=False,
            query_first_value=False,
            time_max_excluded=False,
        )
        self.assertEqual(result, {"timestamp": 3.0, v.id: [[3.0, 2000.0]]})

    def test_variable_read_on_calendar(self):
        """Variable read multiple test"""
        v = self.vo2
        self.assertEqual(v.query_prev_value(), True)

        logger.debug("test_variable_read_on_calendar 1")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old,
            time_in_ms=False,
            query_first_value=True,
            time_max_excluded=True,
        )
        self.assertEqual(result, {"timestamp": 2.0, v.id: [[2.0, 200.0]]})

        logger.debug("test_variable_read_on_calendar 2")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old,
            time_in_ms=False,
            query_first_value=False,
            time_max_excluded=True,
        )
        self.assertEqual(result, {"timestamp": 0})

        logger.debug("test_variable_read_on_calendar 3")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old + 1,
            time_in_ms=False,
            query_first_value=True,
            time_max_excluded=False,
        )
        self.assertEqual(
            result, {"timestamp": 3.0, v.id: [[2.0, 200.0], [3.0, 2000.0]]}
        )

        logger.debug("test_variable_read_on_calendar 4")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old + 1,
            time_in_ms=False,
            query_first_value=False,
            time_max_excluded=False,
        )
        self.assertEqual(result, {"timestamp": 3.0, v.id: [[3.0, 2000.0]]})

        logger.debug("test_variable_read_on_calendar 5")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old,
            time_in_ms=False,
            query_first_value=True,
            time_max_excluded=False,
        )
        self.assertEqual(result, {"timestamp": 2.0, v.id: [[2.0, 200.0]]})

        logger.debug("test_variable_read_on_calendar 6")
        result = Variable.objects.read_multiple(
            variable=v,
            time_min=v.timestamp_old,
            time_max=v.timestamp_old,
            time_in_ms=False,
            query_first_value=False,
            time_max_excluded=False,
        )
        self.assertEqual(
            result,
            {
                "timestamp": 0,
            },
        )

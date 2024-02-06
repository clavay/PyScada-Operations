PyScada Operations Extension
============================

This is a extension for PyScada to do operations on data.


What is Working
---------------

 - define a master operation in de operation device configuration (see below)
 - define a second operation in the operation variable configuration (see below)


What is not Working/Missing
---------------------------

 - Documentation

Master operation
----------------

A master operation of a operation device allows to define a mathematical operation on other variable to assign virtual values to new variables (assigned to this device).
The library used behind is `simpleeval <https://github.com/danthedeckie/simpleeval>`_.
The operation should use a defined format :
 - use the operators, functions and if expresions as allowed by `simpleeval <https://github.com/danthedeckie/simpleeval>`_.
 - refer to a variable last value using variable(id)
 - refer to a variable last timestamp using variable(id, type="get_last_timestamp")

Installation
------------

 - pip install https://github.com/clavay/PyScada-Operations/tarball/main
 - in order to move you CalculatedVariables to AggregationVariables you need to install this plugin before running the pyscada migration 0108.
 - in order to see the aggregation protocol, you need to have the last `settings.py` version which add the aggregation app to `INSTALLED_APPS`. Add [this lines](https://github.com/pyscada/PyScada/blob/main/tests/project_template/project_name/settings.py-tpl#L60-L65) if missing.

Contribute
----------

 - Issue Tracker: https://github.com/clavay/PyScada-Operations/issues
 - Source Code: https://github.com/clavay/PyScada-Operations

 - Format code before sending a pull request :
  - python code using `black <https://black.readthedocs.io>`_
  - django template, JavaScript and CSS using `DjHTML <https://github.com/rtts/djhtml>`_


License
-------

The project is licensed under the _GNU AFFERO GENERAL PUBLIC LICENSE Version 3 (AGPLv3)_.
-

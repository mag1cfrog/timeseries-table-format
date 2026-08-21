# Configure native logging

The Rust table engine sends its diagnostics through Python's standard
`logging` hierarchy. Configure the `timeseries_table_format` logger before the
first table operation to receive records from every project module.

## Add a handler

```python
import logging

logger = logging.getLogger("timeseries_table_format")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)

import timeseries_table_format as ttf

# Native table operations now emit through this logger hierarchy.
```

The package does not call `logging.basicConfig()`, add a handler, change the
root logger, disable propagation, set a formatter, or choose an output
destination. Those choices remain under application control.

Python's normal propagation rules apply. If both this logger and an ancestor
logger have handlers, one record may be displayed by both handlers. Remove one
handler or set `logger.propagate = False` if that is not what you want.

## Logger hierarchy and levels

Project records appear under child loggers such as:

- `timeseries_table_format.table`
- `timeseries_table_format.transaction_log`
- `timeseries_table_format.datafusion`

Configuring the `timeseries_table_format` parent applies to these children
through normal Python logger inheritance.

Project `DEBUG`, `INFO`, `WARNING`, and `ERROR` records can be forwarded.
Third-party dependency records also use the project root, but dependency
`DEBUG` records are filtered because they may contain SQL text or query plans.
`RUST_LOG` does not configure logging for the Python extension.

## Change levels at runtime

Native logger names and effective levels are cached after first use. This lets
disabled records stop before they acquire the GIL. Configuration completed
before the first table operation is honored without an extra step.

After changing Python logger levels later in the process, clear the native
level cache:

```python
logger.setLevel(logging.INFO)
ttf.refresh_logging_cache()
```

The next native record uses the current Python levels. Changing handlers or
formatters does not require a cache refresh.

## Diagnostics and operation errors

Operation exceptions remain the authoritative failure channel. A returned
table, append, optimization, or query error is not generically duplicated as
an error record. Failures inside an application logging handler do not replace
the result of a native operation.

Native diagnostics do not include:

- SQL text or bound parameter values
- entity values or record contents
- complete schemas
- credentials, environment variables, or object-store secrets

Recovery warnings may include table-relative managed paths so the affected
sidecar or segment can be identified.

## Telemetry backends

The package does not install OpenTelemetry, a metrics exporter, a collector,
or another telemetry backend. Applications that need external telemetry can
connect Python logging to their chosen integration.

For missing or duplicated output, see
[Troubleshooting](../troubleshooting.md#native-diagnostics-do-not-appear).

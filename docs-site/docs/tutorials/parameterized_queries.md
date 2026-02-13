# Tutorial: parameterized queries

DataFusion SQL supports placeholders:

- Positional: `$1`, `$2`, ...
- Named: `$name`

This example shows both styles:

```python
--8<-- "python/examples/parameterized_queries.py"
```

## Supported parameter types

For v0, parameter values must be one of:
- `None`, `bool`, `int` (i64 range), `float`, `str`, `bytes`

!!! note
    If you use placeholders in a `SELECT` projection without type context, you may need an explicit
    cast (as shown in the example).


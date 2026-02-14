# Tutorial: parameterized queries

**Goal:** Use placeholders in SQL safely (positional and named) with `Session.sql(...)`.

**Prereqs:** Comfortable running basic `Session.sql(...)` queries (see [Create, append, query](create_append_query.md)).

**What you’ll learn:**
- Positional (`$1`, `$2`, …) vs named (`$name`) placeholders
- Which Python types are supported for parameters in v0

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

Next:
- Concept: [Buckets + overlap](../concepts/bucketing_and_overlap.md)
- Reference: [Exceptions](../reference/exceptions.md)

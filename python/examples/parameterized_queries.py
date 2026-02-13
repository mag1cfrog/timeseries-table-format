from __future__ import annotations

import pyarrow as pa

import timeseries_table_format as ttf


def run() -> list[pa.Table]:
    sess = ttf.Session()

    out_positional = sess.sql(
        "select cast($1 as bigint) as x, cast($2 as varchar) as y",
        params=[1, "hello"],
    )
    out_named = sess.sql(
        "select cast($a as bigint) as x, cast($b as varchar) as y",
        params={"a": 2, "b": "world"},
    )

    return [out_positional, out_named]


def main() -> None:
    out_positional, out_named = run()
    print(out_positional)
    print(out_named)


if __name__ == "__main__":
    main()

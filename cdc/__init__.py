"""CDC capture registry, compiler and developer tooling.

A REAL package, with relative imports, on purpose. `spark/reporting/` uses flat module
names (`models`, `config_loader`, `plan`) and is imported by putting its directory on
sys.path. This package deliberately mirrors its SHAPE but must not mirror that mechanism:
when `cdc/` was also flat, putting it on sys.path shadowed the reporting modules and broke
19 unrelated test files at collection. Relative imports make the collision impossible.
"""

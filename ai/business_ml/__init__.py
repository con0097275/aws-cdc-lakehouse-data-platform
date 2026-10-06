"""Business feature materialisation and predictive pilots (BAI-P5).

NAMED `business_ml`, NOT `ml`. `spark/ml/` already exists and `spark/` is on the test path,
so a package called `ml` here is SHADOWED by it: this suite passed in isolation and failed
the moment the full suite ran, with `No module named 'ml.features'`. The same shadowing
took out the Session-16 assistant once before, when `ai/tools/` hid `ai/tools.py`.
"""

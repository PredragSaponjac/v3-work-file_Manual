"""
ORCA v20 — Next-generation options intelligence layer.

Sits on top of the existing v3 pipeline. All v3 files remain untouched.
v20 introduces: thesis persistence, momentum tracking, institutional pressure,
elite-agent simulation, evidence gates, causal/quant/factor gates,
Kelly sizing, execution-impact modeling, and daemon kill-switches.

Database: orca_v20.db (completely separate from v3 databases)
Entrypoint: pipeline_v20.py
"""

__version__ = "0.1.0"

"""Data generator that simulates multi-Channel sales data for the Seller ACME.

Three layers (see docs/data-platform/reference/generator_design.md, section 2):
- `generator.core`: simulation core, generates the shared "truth" in no Channel's shape.
- `generator.channels`: renderers, draw the truth in each Channel's native shape.
- `generator.delivery`: delivery layer, injects faults (duplicates, delays, batching) last.
"""

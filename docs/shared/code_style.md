# Code conventions

Goal: a newcomer who opens a file can read it without wading through an essay.

## Tools

- `uv run ruff format .` handles line wrapping, indentation and quotes (max line length 100 characters).
- `uv run ruff check --fix .` sorts imports, removes unused imports and catches a few common mistakes.
- Both run in pre-commit and CI (`.github/workflows/ci.yml`). Configuration is in `pyproject.toml`.

## Comments

- **File header**: one line saying what the file does. Add a few lines for things the code cannot show,
  and how to run it if it is a job.
- **Function docstring**: one line is enough. Add a second paragraph only when there is a non-obvious "why".
- **Comments**: explain *why*, do not repeat *what the code does*.
- One file uses one language (English).

Do not put in code:

- Line numbers in other files (`walmart.py:103`) or doc section numbers: they go stale as soon as the other file changes.
  Pointing to the doc name is enough, for example `docs/data-platform/reference/silver.md`.
- Decision history ("decided on ...", "Q3"): keep it in `docs/` or an ADR.
- Measurements from a single run (GB, row counts, seconds): keep them in the evidence docs.
- ALL CAPS for emphasis.
- Names of people/tools doing the work (operator, brief, ...).

## Example

Before:

```python
def dedup(df):
    """Drop duplicates by key `(order_id, order_item_id)`, keeping the newest record.

    Tie-break order, decided in `docs/data-platform/reference/silver.md` section 3.1:
      1. largest `last_updated_time` — "always take the newest";
      2. on a tie, largest `export_date`.

    Why `order_id` ALONE is wrong: at the Order Line level, one `order_id` spans many REAL
    rows (one order, several items), so dedup by `order_id` alone would delete real Order Lines. ...
    """
```

After:

```python
def dedup(df):
    """Keep the newest record of each `(order_id, order_item_id)`.

    Order: largest `last_updated_time`, on a tie largest `export_date`. The key must include
    `order_item_id`: using `order_id` alone would wrongly delete other lines of the same order.
    """
```

"""Compare schema evolution; migrations explain renames, they do not waive compatibility rules."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Change:
    """One change at a JSON path and its required-ness on both sides."""

    kind: str
    path: str
    old_required: bool = False
    new_required: bool = False
    target: str | None = None


def diff(old, new, migrations=None, path="$"):
    """Compare properties recursively, including array items."""
    migrations = migrations or {}
    changes = []
    if old.get("type") != new.get("type"):
        changes.append(Change("type_changed", path))
    op, np = old.get("properties", {}), new.get("properties", {})
    ore, nre = old.get("required", []), new.get("required", [])
    renamed_targets = set()
    for key in sorted(op.keys() - np.keys()):
        source = f"{path}.{key}"
        target = migrations.get(source)
        target_key = target.rsplit(".", 1)[-1] if target else None
        if (
            target
            and target.rsplit(".", 1)[0] == path
            and target_key in np
            and target_key not in op
        ):
            changes.append(Change("field_renamed", source, key in ore, target_key in nre, target))
            changes.extend(diff(op[key], np[target_key], migrations, target))
            renamed_targets.add(target_key)
        else:
            changes.append(Change("field_removed", source, key in ore))
    for key in sorted(np.keys() - op.keys() - renamed_targets):
        changes.append(
            Change(
                "field_added_required" if key in nre else "field_added_optional",
                f"{path}.{key}",
                False,
                key in nre,
            )
        )
    for key in sorted(op.keys() & np.keys()):
        if (key in ore) != (key in nre):
            changes.append(
                Change(
                    "became_required" if key in nre else "became_optional",
                    f"{path}.{key}",
                    key in ore,
                    key in nre,
                )
            )
        changes.extend(diff(op[key], np[key], migrations, f"{path}.{key}"))
    if "items" in old or "items" in new:
        changes.extend(diff(old.get("items", {}), new.get("items", {}), migrations, path + "[0]"))
    return changes


def verdict(changes, mode):
    """True when the new/old reader definition for the mode is satisfied."""
    if mode not in {"BACKWARD", "FORWARD", "FULL", "NONE"}:
        raise ValueError(f"Unknown compatibility mode: {mode}")
    backward = any(
        c.kind in {"field_added_required", "type_changed", "became_required"}
        or (c.kind == "field_renamed" and c.new_required)
        for c in changes
    )
    forward = any(
        c.kind == "type_changed"
        or (c.kind in {"field_removed", "field_renamed", "became_optional"} and c.old_required)
        for c in changes
    )
    return not (
        (mode in {"BACKWARD", "FULL"} and backward) or (mode in {"FORWARD", "FULL"} and forward)
    )

#!/usr/bin/env python3
"""Small YAML config loader with optional PyYAML support."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple


def _parse_scalar(value: str) -> Any:
    raw = value.strip()
    if raw == "":
        return ""
    if raw.startswith(("'", '"')) and raw.endswith(("'", '"')) and len(raw) >= 2:
        return raw[1:-1]

    lower = raw.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower in {"null", "none"}:
        return None

    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def _strip_inline_comment(value: str) -> str:
    text = value.rstrip()
    if text.startswith(("'", '"')):
        return text
    marker = text.find(" #")
    if marker >= 0:
        return text[:marker].rstrip()
    return text


def _simple_yaml_parse(text: str) -> Dict[str, Any]:
    root: Dict[str, Any] = {}
    stack: List[Tuple[int, Dict[str, Any]]] = [(-1, root)]

    for raw in text.splitlines():
        if not raw.strip():
            continue
        stripped_leading = raw.lstrip(" ")
        if stripped_leading.startswith("#"):
            continue

        indent = len(raw) - len(stripped_leading)
        line = stripped_leading
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = _strip_inline_comment(value.strip())
        if not key:
            continue

        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1] if stack else root

        if value == "":
            node: Dict[str, Any] = {}
            parent[key] = node
            stack.append((indent, node))
        else:
            parent[key] = _parse_scalar(value)

    return root


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8")

    try:
        import yaml  # type: ignore

        loaded = yaml.safe_load(text)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return _simple_yaml_parse(text)

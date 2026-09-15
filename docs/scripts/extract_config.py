#!/usr/bin/env python3
"""
Extract config keys, types, and defaults from Scala source files.

Usage:
    python3 docs/scripts/extract_config.py > docs/CONFIG_REFERENCE.md

This scans the ingestion framework source code for ConfigUtils calls and
direct Config API usage, extracting the config key paths, types, and default
values to generate a configuration reference.
"""

import re
import os
from collections import defaultdict
from pathlib import Path

# Repository root (relative to this script's location)
SCRIPT_DIR = Path(__file__).parent
REPO = SCRIPT_DIR.parent.parent

PATTERNS = [
    # ConfigUtils.optString(conf, "key").getOrElse("default")
    (r'ConfigUtils\.opt(String|Int|Long|Boolean|Double)\s*\(\s*\w+\s*,\s*"([^"]+)"\s*\)\.getOrElse\s*\(\s*([^)]+)\s*\)',
     lambda m: (m.group(2), m.group(1).lower(), m.group(3).strip())),

    # ConfigUtils.optString(conf, "key") - optional, no default
    (r'ConfigUtils\.opt(String|Int|Long|Boolean|Double)\s*\(\s*\w+\s*,\s*"([^"]+)"\s*\)(?!\.getOrElse)',
     lambda m: (m.group(2), m.group(1).lower(), None)),

    # ConfigUtils.optConfig(conf, "key")
    (r'ConfigUtils\.optConfig\s*\(\s*\w+\s*,\s*"([^"]+)"\s*\)',
     lambda m: (m.group(1), 'config', None)),

    # conf.getString("key") - required
    (r'\.getString\s*\(\s*"([^"]+)"\s*\)',
     lambda m: (m.group(1), 'string', '<required>')),

    # conf.getInt("key") - required
    (r'\.getInt\s*\(\s*"([^"]+)"\s*\)',
     lambda m: (m.group(1), 'int', '<required>')),

    # conf.getBoolean("key") - required
    (r'\.getBoolean\s*\(\s*"([^"]+)"\s*\)',
     lambda m: (m.group(1), 'boolean', '<required>')),

    # conf.getLong("key") - required
    (r'\.getLong\s*\(\s*"([^"]+)"\s*\)',
     lambda m: (m.group(1), 'long', '<required>')),
]

# Keys to exclude (internal/non-config)
EXCLUDE_KEYS = {
    'COLUMN_NAME', 'COLUMN_SIZE', 'DATA_TYPE', 'DECIMAL_DIGITS',
    'NULLABLE', 'ORDINAL_POSITION', 'TABLE_NAME', 'TYPE_NAME',
    'id', 'scalar', 'items', 'blocks', 'source_type', 'answers',
}


def extract_from_file(filepath):
    """Extract config keys from a single file."""
    results = []
    try:
        content = filepath.read_text()
        for pattern, extractor in PATTERNS:
            for match in re.finditer(pattern, content):
                key, typ, default = extractor(match)
                if key in EXCLUDE_KEYS:
                    continue
                # Get line number for context
                line_start = content[:match.start()].count('\n') + 1
                results.append({
                    'key': key,
                    'type': typ,
                    'default': default,
                    'file': str(filepath.relative_to(REPO)),
                    'line': line_start
                })
    except Exception as e:
        print(f"# Error reading {filepath}: {e}", file=__import__('sys').stderr)
    return results


def categorize_key(key):
    """Categorize a key by its section."""
    parts = key.split('.')
    if len(parts) == 1:
        return 'misc'

    section = parts[0]
    if section in ['app', 'source', 'raw', 'curated', 'audit', 'rejects',
                   'schema', 'concurrency', 'notifications', 'reconcile',
                   'checkpoint', 'extraction', 'validation', 'idempotency',
                   'ingestion', 'deletes', 'watermark', 'retention']:
        return section
    return 'misc'


def main():
    all_configs = defaultdict(dict)  # section -> key -> info

    # Scan main source files only
    for module in ['ingestion-core', 'ingestion-app', 'ingestion-jdbc', 'ingestion-config-gen']:
        src_dir = REPO / module / 'src' / 'main' / 'scala'
        if not src_dir.exists():
            continue
        for scala_file in src_dir.rglob('*.scala'):
            for config in extract_from_file(scala_file):
                key = config['key']
                section = categorize_key(key)

                # Keep the entry with the most info (prefer ones with defaults)
                existing = all_configs[section].get(key)
                if not existing or (config['default'] and not existing.get('default')):
                    all_configs[section][key] = config

    # Output as markdown (raw format for reference)
    print("# Configuration Keys (Raw Extraction)")
    print()
    print("Auto-extracted config keys from source code.")
    print()
    print("Legend:")
    print("- `<required>` — must be set, no default")
    print("- `<optional>` — can be omitted (typically null/empty)")
    print("- Value shown — default if omitted")
    print()

    section_order = ['app', 'source', 'extraction', 'checkpoint', 'schema',
                     'validation', 'raw', 'curated', 'audit', 'rejects',
                     'reconcile', 'concurrency', 'notifications', 'ingestion',
                     'watermark', 'retention', 'deletes', 'idempotency', 'misc']

    for section in section_order:
        if section not in all_configs:
            continue
        configs = all_configs[section]
        if not configs:
            continue

        print(f"## {section.title()} (`{section}.*`)")
        print()
        print("| Key | Type | Default | Source |")
        print("|-----|------|---------|--------|")

        for key in sorted(configs.keys()):
            info = configs[key]
            default = info['default'] or '<optional>'
            # Clean up default display
            if default.endswith('L'):
                default = default[:-1]
            if default.startswith('"') and default.endswith('"'):
                default = default[1:-1]

            src = f"{info['file']}:{info['line']}"
            # Shorten source path
            src = src.replace('ingestion-', '').replace('/src/main/scala/com/hcsc/generic/ingest/', ':')

            print(f"| `{key}` | {info['type']} | `{default}` | {src} |")

        print()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Strip all comments from .rs and Cargo.toml files in the project.

.rs files:  removes // line comments and /* */ block comments (nested).
Cargo.toml: removes # line comments.

Respects string literals — does not touch comment-like text inside strings.

Usage:
    python3 strip_comments.py [--dry-run] [root_dir]
"""

import os
import sys


def remove_rust_comments(text):
    """Remove all comments from Rust source, preserving newlines."""
    out = []
    i = 0
    n = len(text)

    while i < n:
        # ── raw string literal: r"", r#""#, br"", br#""# ──
        if text[i] in ('b', 'r'):
            j = i
            if text[j] == 'b':
                j += 1
            if j < n and text[j] == 'r':
                k = j + 1
                hashes = 0
                while k < n and text[k] == '#':
                    hashes += 1
                    k += 1
                if k < n and text[k] == '"':
                    closing = '"' + '#' * hashes
                    k += 1  # skip opening "
                    pos = text.find(closing, k)
                    if pos < 0:
                        out.append(text[i:])
                        return ''.join(out)
                    end = pos + len(closing)
                    out.append(text[i:end])
                    i = end
                    continue
            # not a raw string — fall through

        c = text[i]

        # ── line comment // ──
        if c == '/' and i + 1 < n and text[i + 1] == '/':
            # strip trailing whitespace before comment
            while out and out[-1] in (' ', '\t'):
                out.pop()
            while i < n and text[i] != '\n':
                i += 1
            continue

        # ── block comment /* */ (Rust allows nesting) ──
        if c == '/' and i + 1 < n and text[i + 1] == '*':
            depth = 1
            i += 2
            while i < n and depth > 0:
                if text[i] == '/' and i + 1 < n and text[i + 1] == '*':
                    depth += 1
                    i += 2
                elif text[i] == '*' and i + 1 < n and text[i + 1] == '/':
                    depth -= 1
                    i += 2
                else:
                    if text[i] == '\n':
                        out.append('\n')  # preserve newline count
                    i += 1
            continue

        # ── regular string literal "..." ──
        if c == '"':
            out.append(c)
            i += 1
            while i < n and text[i] != '"':
                if text[i] == '\\':
                    out.append(text[i])
                    i += 1
                    if i < n:
                        out.append(text[i])
                        i += 1
                else:
                    out.append(text[i])
                    i += 1
            if i < n:
                out.append(text[i])  # closing "
                i += 1
            continue

        # ── everything else ──
        out.append(c)
        i += 1

    return ''.join(out)


def remove_toml_comments(text):
    """Remove # line comments from TOML, respecting string literals."""
    out = []
    i = 0
    n = len(text)

    while i < n:
        c = text[i]

        # ── multi-line basic string \"\"\" ──
        if text[i:i + 3] == '"""':
            end = text.find('"""', i + 3)
            end = end + 3 if end >= 0 else n
            out.append(text[i:end])
            i = end
            continue

        # ── multi-line literal string ''' ──
        if text[i:i + 3] == "'''":
            end = text.find("'''", i + 3)
            end = end + 3 if end >= 0 else n
            out.append(text[i:end])
            i = end
            continue

        # ── basic string "..." ──
        if c == '"':
            out.append(c)
            i += 1
            while i < n and text[i] != '"':
                if text[i] == '\\':
                    out.append(text[i])
                    i += 1
                    if i < n:
                        out.append(text[i])
                        i += 1
                else:
                    out.append(text[i])
                    i += 1
            if i < n:
                out.append(text[i])
                i += 1
            continue

        # ── literal string '...' ──
        if c == "'":
            out.append(c)
            i += 1
            while i < n and text[i] != "'":
                out.append(text[i])
                i += 1
            if i < n:
                out.append(text[i])
                i += 1
            continue

        # ── comment # ──
        if c == '#':
            # strip trailing whitespace before comment
            while out and out[-1] in (' ', '\t'):
                out.pop()
            while i < n and text[i] != '\n':
                i += 1
            continue

        out.append(c)
        i += 1

    return ''.join(out)


def cleanup(original, processed):
    """
    Compare original and processed line-by-line.
    - Lines that became blank due to comment removal are deleted.
    - Lines that were modified get trailing whitespace stripped.
    - Untouched lines are preserved exactly.
    """
    orig_lines = original.split('\n')
    proc_lines = processed.split('\n')

    if len(orig_lines) != len(proc_lines):
        # newline count mismatch (shouldn't happen) — simple fallback
        return processed

    result = []
    for o, p in zip(orig_lines, proc_lines):
        if o == p:
            # unchanged line — keep as-is
            result.append(p)
        elif p.strip() == '':
            # line became blank after comment removal — drop it
            continue
        else:
            # line was modified — strip trailing whitespace
            result.append(p.rstrip())

    return '\n'.join(result)


def process_file(path, dry_run=False):
    with open(path, 'r', encoding='utf-8') as f:
        original = f.read()

    if not original.strip():
        return False

    if path.endswith('.rs'):
        processed = remove_rust_comments(original)
    elif path.endswith('Cargo.toml'):
        processed = remove_toml_comments(original)
    else:
        return False

    result = cleanup(original, processed)

    if result == original:
        return False

    if dry_run:
        print(f"  [dry-run] would modify: {path}")
        return True

    with open(path, 'w', encoding='utf-8') as f:
        f.write(result)
    return True


def find_files(root):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith('.') and d != 'target']
        for fname in sorted(filenames):
            if fname.endswith('.rs') or fname == 'Cargo.toml':
                yield os.path.join(dirpath, fname)


def main():
    dry_run = '--dry-run' in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    root = args[0] if args else '.'

    changed = 0
    total = 0
    for path in sorted(find_files(root)):
        total += 1
        if process_file(path, dry_run=dry_run):
            changed += 1
            if not dry_run:
                print(f"  modified: {path}")

    print(f"\n{changed}/{total} files {'would be ' if dry_run else ''}modified.")


if __name__ == '__main__':
    main()

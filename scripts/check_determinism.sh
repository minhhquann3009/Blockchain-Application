#!/usr/bin/env bash
# Spec section 8: "Provide a script that runs the same scenario twice and
# checks equality of logs and final state."
#
# `python main.py --test 8` already does this in-process. This script is the
# stricter, external version: it launches TWO SEPARATE Python processes and
# diffs the log files on disk, so nothing carried over in memory can mask a
# nondeterminism. A fresh interpreter each time also means a different
# PYTHONHASHSEED, catching anything that depends on set/dict hash ordering.
set -u

SCENARIO="${1:-1}"
# T8 runs the scenario twice internally and writes two log files; every other
# scenario writes a single logs/tN.jsonl.
if [ "$SCENARIO" = "8" ]; then
    LOG="logs/t8_run1.jsonl"
else
    LOG="logs/t${SCENARIO}.jsonl"
fi

cd "$(dirname "$0")/.." || exit 2

echo "Determinism check: scenario T${SCENARIO}, two separate processes"

python3 main.py --test "$SCENARIO" > /tmp/det_run1.out 2>&1 || {
    echo "FAIL: run 1 exited non-zero"; cat /tmp/det_run1.out; exit 1; }
cp "$LOG" /tmp/det_run1.jsonl

python3 main.py --test "$SCENARIO" > /tmp/det_run2.out 2>&1 || {
    echo "FAIL: run 2 exited non-zero"; cat /tmp/det_run2.out; exit 1; }
cp "$LOG" /tmp/det_run2.jsonl

size1=$(wc -c < /tmp/det_run1.jsonl)
size2=$(wc -c < /tmp/det_run2.jsonl)
echo "  run 1: ${size1} bytes"
echo "  run 2: ${size2} bytes"

if diff -q /tmp/det_run1.jsonl /tmp/det_run2.jsonl > /dev/null; then
    echo "PASS: logs are byte-identical across processes"
else
    echo "FAIL: logs differ. First difference:"
    diff /tmp/det_run1.jsonl /tmp/det_run2.jsonl | head -6
    exit 1
fi

# The final state hash is printed by the scenario itself; compare the whole
# stdout, which includes heights, head hashes and the state root.
if diff -q /tmp/det_run1.out /tmp/det_run2.out > /dev/null; then
    echo "PASS: reported final state identical across processes"
else
    echo "FAIL: reported final state differs:"
    diff /tmp/det_run1.out /tmp/det_run2.out | head -10
    exit 1
fi

echo "Determinism verified for T${SCENARIO}."

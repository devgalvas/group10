"""Task counts and spill per experiment variant, read from Spark event logs (no Spark needed).

The harness (perf.harness) records the REST totals, where spill is memory + disk bytes. This
script reads the event log of a run and reports, per variant (job group without its nonce):
- the bytes spilled to memory and to disk, summed over every task of every run of the variant
  (warm-up included: divide by runs + 1 for a per-run figure);
- the task count of each stage that reads at least 1 MB of shuffle data (after AQE coalescing).

Usage (on the VM):
    python3 -m perf.eventlog /data/spark-events/local-<app-id> [...]
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field

MB = 2**20


@dataclass
class Group:
    memory_spilled: int = 0
    disk_spilled: int = 0
    # stage id -> [tasks, shuffle bytes read, disk bytes spilled]
    stages: dict[int, list[int]] = field(default_factory=dict)


def _variant(job_group: str) -> str:
    """'P4:silver_sp16_coalesce_on:<nonce>' -> 'P4:silver_sp16_coalesce_on'."""
    return job_group.rsplit(":", 1)[0] if job_group.count(":") >= 2 else job_group


def read(path: str) -> dict[str, Group]:
    stage_group: dict[int, str] = {}
    groups: dict[str, Group] = defaultdict(Group)
    with open(path) as f:
        for line in f:
            ev = json.loads(line)
            kind = ev.get("Event")
            if kind == "SparkListenerJobStart":
                job_group = (ev.get("Properties") or {}).get("spark.jobGroup.id", "-")
                for sid in ev.get("Stage IDs", []):
                    stage_group[sid] = _variant(job_group)
            elif kind == "SparkListenerTaskEnd":
                sid = ev["Stage ID"]
                g = groups[stage_group.get(sid, "-")]
                m = ev.get("Task Metrics") or {}
                sr = m.get("Shuffle Read Metrics") or {}
                g.memory_spilled += m.get("Memory Bytes Spilled", 0)
                g.disk_spilled += m.get("Disk Bytes Spilled", 0)
                st = g.stages.setdefault(sid, [0, 0, 0])
                st[0] += 1
                st[1] += sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0)
                st[2] += m.get("Disk Bytes Spilled", 0)
    return groups


def main(argv: list[str]) -> int:
    for path in argv:
        print(f"## {path}")
        for name, g in sorted(read(path).items()):
            if name == "-":
                continue
            print(
                f"{name}: memory spilled {g.memory_spilled / MB:,.0f} MB, "
                f"disk spilled {g.disk_spilled / MB:,.0f} MB"
            )
            for sid, (tasks, shuffle, disk) in sorted(g.stages.items()):
                if shuffle >= MB:  # skip tiny shuffles (counts, broadcasts)
                    print(
                        f"  stage {sid}: {tasks} tasks, shuffle read {shuffle / MB:,.0f} MB, "
                        f"disk spilled {disk / MB:,.0f} MB"
                    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

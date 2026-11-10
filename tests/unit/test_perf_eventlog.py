import json

from perf.eventlog import read


def _task(stage: int, shuffle: int = 0, mem: int = 0, disk: int = 0) -> dict:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage,
        "Task Metrics": {
            "Memory Bytes Spilled": mem,
            "Disk Bytes Spilled": disk,
            "Shuffle Read Metrics": {"Remote Bytes Read": 0, "Local Bytes Read": shuffle},
        },
    }


def test_groups_spill_and_shuffle_tasks_by_variant(tmp_path):
    events = [
        {
            "Event": "SparkListenerJobStart",
            "Stage IDs": [1, 2],
            "Properties": {"spark.jobGroup.id": "P4:silver_sp16_coalesce_on:123"},
        },
        {"Event": "SparkListenerJobStart", "Stage IDs": [3], "Properties": {}},
        _task(1, mem=10, disk=1),
        _task(2, shuffle=5, mem=20, disk=2),
        _task(2, shuffle=5),
        _task(3, shuffle=7, mem=99, disk=99),
    ]
    log = tmp_path / "local-1"
    log.write_text("\n".join(json.dumps(e) for e in events) + "\n")

    groups = read(str(log))

    g = groups["P4:silver_sp16_coalesce_on"]
    assert (g.memory_spilled, g.disk_spilled) == (30, 3)
    assert g.stages[2] == [2, 10, 2]  # 2 tasks, 10 bytes of shuffle read, 2 bytes to disk
    assert g.stages[1][1] == 0  # reads no shuffle
    assert groups["-"].disk_spilled == 99  # stages outside a job group stay apart

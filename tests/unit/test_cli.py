import pytest

from ais_pipeline.cli import build_parser, main


def test_parser_reads_range():
    args = build_parser().parse_args(
        ["--env", "ci", "bronze", "--start", "2026-09-01", "--end", "2026-09-02", "--force"]
    )
    assert args.command == "bronze"
    assert args.start.isoformat() == "2026-09-01"
    assert args.force


def test_parser_rejects_bad_date():
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["--env", "ci", "silver", "--start", "01/09/2026", "--end", "2026-09-01"]
        )


def test_unimplemented_command_exits_1(monkeypatch, tmp_path):
    monkeypatch.setenv("AIS_DATA_ROOT", str(tmp_path))
    assert main(["--env", "ci", "gold"]) == 1

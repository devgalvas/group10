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


def test_date_range_is_inclusive():
    from datetime import date

    from ais_pipeline.cli import date_range

    days = list(date_range(date(2026, 9, 30), date(2026, 10, 2)))
    assert [d.isoformat() for d in days] == ["2026-09-30", "2026-10-01", "2026-10-02"]


def test_date_range_rejects_reversed():
    from datetime import date

    from ais_pipeline.cli import date_range

    with pytest.raises(ValueError, match="before"):
        list(date_range(date(2026, 9, 2), date(2026, 9, 1)))

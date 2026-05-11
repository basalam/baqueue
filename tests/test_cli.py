"""Tests for the click-based CLI."""

from __future__ import annotations

from click.testing import CliRunner

from baqueue.cli import cli


class TestHelp:
    def test_root_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "work" in result.output
        assert "dashboard" in result.output
        assert "retry-failed" in result.output
        assert "prune" in result.output

    def test_retry_failed_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["retry-failed", "--help"])
        assert result.exit_code == 0
        assert "--queue" in result.output
        assert "--tag" in result.output
        assert "--hours" in result.output
        assert "--yes" in result.output

    def test_work_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["work", "--help"])
        assert result.exit_code == 0
        assert "--workers" in result.output

    def test_prune_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["prune", "--help"])
        assert result.exit_code == 0
        assert "--status" in result.output

    def test_status_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["status", "--help"])
        assert result.exit_code == 0


class TestInvalidDriver:
    def test_status_with_unknown_driver_fails(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["status", "-d", "nonexistent"])
        assert result.exit_code != 0
        assert "Unknown driver" in (result.output + (result.stderr_bytes or b"").decode("utf-8", "ignore"))


class TestRetryFailedAbort:
    def test_aborted_without_confirm(self):
        runner = CliRunner()
        # No -y, simulate user saying "no"
        result = runner.invoke(cli, ["retry-failed", "-d", "memory"], input="n\n")
        # Aborted exit code is 1 from click
        assert result.exit_code != 0

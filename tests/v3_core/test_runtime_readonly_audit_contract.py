"""Safety contract for the existing fixed-host readonly V3 audit workflow."""

import ast
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import textwrap

WORKFLOW = (
    Path(__file__).parents[2]
    / ".github/workflows/v3-readonly-runtime-audit-20261106.yml"
)


def _script():
    text = WORKFLOW.read_text()
    return textwrap.dedent(
        text.split("/usr/bin/python3 -I - <<'PYRO'\n", 1)[1].split(
            "\n          PYRO", 1
        )[0]
    )


def test_audit_does_not_accept_commands_or_dump_env_logs_or_mutate_services():
    text = WORKFLOW.read_text()
    for forbidden in (
        "inputs:",
        "/proc/",
        "systemctl restart",
        "systemctl stop",
        "workflow_run:",
        "pull_request_target:",
    ):
        assert forbidden not in text
    script = _script()
    ast.parse(script)
    for forbidden in (
        "os.environ",
        "getenv(",
        "shell=True",
        "db.commit",
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "CREATE ",
        "ALTER ",
    ):
        assert forbidden not in script
    assert "?mode=ro" in script and "PRAGMA query_only=ON" in script
    assert "contents: read" in text
    assert "50.114.74.120" in text
    assert "SHA256:fuufBlFaqRpQhlkio0vuLsuv8w8x3zUb3VFQsnUny/o" in text
    assert "secrets.PROD_SSH_PRIVATE_KEY" in text
    assert script.count('Path("/opt/qiaolian_v3/runtime.env").open') == 1
    assert "/getChat?" in script and "sendMessage" not in script
    assert "--initialize" not in script.replace(
        "# Existing deployed preflight is statically readonly, without --initialize.",
        "",
    )
    assert "actions/checkout" not in text  # Do not import or run PR code on production.


def test_fixed_audit_executes_only_whitelisted_reads_and_emits_no_row_data(
    tmp_path, monkeypatch, capsys
):
    import os
    import subprocess

    db = tmp_path / "audit.db"
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """CREATE TABLE publisher_autopilot_settings_v3(setting_key TEXT,setting_value TEXT);
          INSERT INTO publisher_autopilot_settings_v3 VALUES ('enabled','true');
          CREATE TABLE publisher_settings_v3(setting_key TEXT,setting_value TEXT);
          INSERT INTO publisher_settings_v3 VALUES ('daily_broadcast_enabled','0');
          INSERT INTO publisher_settings_v3 VALUES ('marketing_broadcast_enabled','1');
          CREATE TABLE publisher_auto_items_v3(state TEXT);
          INSERT INTO publisher_auto_items_v3 VALUES ('queued');
          CREATE TABLE publication_delivery_attempts_v3(state TEXT);
          INSERT INTO publication_delivery_attempts_v3 VALUES ('unknown');
          CREATE TABLE publisher_status_sync_outbox_v3(listing_id TEXT);
          INSERT INTO publisher_status_sync_outbox_v3 VALUES ('PRIVATE_LISTING_SHOULD_NOT_BE_PRINTED');"""
        )
    real_connect = sqlite3.connect

    def readonly(database, **kwargs):
        assert database == "file:/opt/qiaolian_v3/data/qiaolian_dual_bot.db?mode=ro"
        assert kwargs.get("uri") is True
        return real_connect(f"file:{db}?mode=ro", uri=True)

    real_resolve, real_stat, real_is_file = Path.resolve, Path.stat, Path.is_file

    def resolve(path, *args, **kwargs):
        if str(path) == "/opt/qiaolian_v3/current":
            return Path("/opt/qiaolian_v3/releases/" + "a" * 40)
        if str(path).startswith("/opt/") and str(path).endswith(".db"):
            return path
        return real_resolve(path, *args, **kwargs)

    def stat(path, *args, **kwargs):
        if str(path).startswith("/opt/"):
            return SimpleNamespace(st_dev=1, st_ino=2, st_mode=0o100644)
        return real_stat(path, *args, **kwargs)

    def is_file(path):
        return True if str(path).startswith("/opt/") else real_is_file(path)

    calls = []

    def run(args, **kwargs):
        calls.append(args)
        assert kwargs.get("capture_output")
        if args[0] == "runuser":
            assert (
                kwargs["check"] is False and "-B" in args and "--initialize" not in args
            )
            assert kwargs["env"]["PYTHONDONTWRITEBYTECODE"] == "1"
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps(
                    {
                        "ok": True,
                        "checks": [
                            {
                                "name": "storage:tables",
                                "ok": True,
                                "detail": "PRIVATE_PREFLIGHT_DETAIL",
                            }
                        ],
                    }
                ),
            )
        assert kwargs.get("check")
        if args == ["tesseract", "--list-langs"]:
            return SimpleNamespace(stdout="eng\nchi_sim")
        if args[:4] == ["sudo", "-u", "qiaolianbot", "git"]:
            assert args[-2:] == ["rev-parse", "HEAD"]
            return SimpleNamespace(stdout="a" * 40)
        if args[0] == "journalctl":
            assert (
                args[-3:] == ["-n", "500", "--output=cat"] and "10 minutes ago" in args
            )
            return SimpleNamespace(
                stdout="PRIVATE_LISTING_SHOULD_NOT_BE_PRINTED\nTraceback PRIVATE_ERROR"
            )
        assert args[0:2] == ["systemctl", "show"]
        assert args[2] in {
            "qiaolian-v3@" + n + ".service"
            for n in ("collector", "canonical", "publisher", "user")
        }
        assert args[3:] == ["-p", args[4], "--value"] and args[4] in {
            "ActiveState",
            "ExecStart",
            "MainPID",
            "NRestarts",
        }
        if args[4] in {"MainPID", "NRestarts"}:
            return SimpleNamespace(stdout="123" if args[4] == "MainPID" else "0")
        name = args[2].split("@")[1].split(".")[0]
        return SimpleNamespace(
            stdout=(
                "active"
                if args[4] == "ActiveState"
                else "{ path=/bin/bash ; argv[]=/bin/bash /opt/qiaolian_v3/current/deploy/v3/run_component.sh "
                + name
                + " ; }"
            )
        )

    with monkeypatch.context() as m:
        m.setattr(sqlite3, "connect", readonly)
        m.setattr(Path, "resolve", resolve)
        m.setattr(Path, "stat", stat)
        m.setattr(Path, "is_file", is_file)
        m.setattr(os.path, "samefile", lambda a, b: True)
        m.setattr(subprocess, "run", run)
        exec(compile(_script(), "<fixed-audit>", "exec"), {})
    output = capsys.readouterr().out
    report = json.loads(output)
    assert report["current_sha"] == "a" * 40 and report["database_same_inode"] is True
    assert all(
        v["active"] and v["exec_start"]["component_matches"]
        for v in report["services"].values()
    )
    assert report["switches"]["property_autopublish"] == {
        "present": True,
        "enabled": True,
    }
    assert report["switches"]["daily_broadcast"]["enabled"] is False
    assert report["queues"]["unknown_delivery"] == 1
    assert report["queues"]["publisher_status_sync_outbox_v3"] == 1
    assert "PRIVATE_LISTING" not in output and "PRIVATE_PREFLIGHT_DETAIL" not in output
    assert report["live_preflight"]["ok"] and report["ocr"] == {
        "eng": True,
        "chi_sim": True,
    }
    assert report["database_quick_check_ok"] is True
    assert report["recent_error_lines"] == 1 and "PRIVATE_ERROR" not in output
    assert len(calls) == 20

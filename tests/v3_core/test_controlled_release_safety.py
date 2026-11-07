"""Exercise fixed audit helpers and held-mode release controls without host/network access."""

import ast
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage

ROOT = Path(__file__).parents[2]
DEPLOY = ROOT / ".github/workflows/qiaolian-production-deploy.yml"
AUDIT = ROOT / ".github/workflows/v3-readonly-runtime-audit-20261106.yml"


def audit_functions():
    source = textwrap.dedent(
        AUDIT.read_text()
        .split("/usr/bin/python3 -I - <<'PYRO'\n", 1)[1]
        .split("\n          PYRO", 1)[0]
    )
    module = ast.parse(source)
    module.body = [
        node
        for node in module.body
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef))
        or isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "PUBLIC_CONFIG_KEYS"
            for t in node.targets
        )
    ]
    namespace = {}
    exec(compile(module, "<readonly-audit-functions>", "exec"), namespace)
    return namespace


def remote_script():
    return textwrap.dedent(
        DEPLOY.read_text().split("<<'REMOTE'\n", 1)[1].split("\n          REMOTE", 1)[0]
    )


def test_deploy_shell_parses_and_gate_is_not_narrowed():
    source = remote_script()
    subprocess.run(
        ["bash", "-n"], input=source, text=True, check=True, capture_output=True
    )
    text = DEPLOY.read_text()
    assert "python -m pytest -q tests/v3_core" in text
    assert text.index("python -m pytest -q tests/v3_core") < text.index(
        "Configure production SSH"
    )
    assert "hold_publisher:" in text and "default: true" in text
    assert "inputs.confirm == 'DEPLOY'" in text
    assert "git merge" not in text and "git push" not in text
    assert "raw_output_suppressed=true" in text
    assert "printf '%s\\n' \"$LOGS\"\n" not in text


@pytest.mark.parametrize(
    "held,previously_held,expected",
    [("true", 0, False), ("false", 1, False), ("false", 0, True)],
)
def test_real_rollback_function_never_restores_database_or_restarts_held_publisher(
    tmp_path, held, previously_held, expected
):
    source = remote_script()
    function = (
        "rollback_release() {"
        + source.split("rollback_release() {", 1)[1].split("\ntrap cleanup EXIT", 1)[0]
    )
    old = tmp_path / "old"
    old.mkdir()
    db = tmp_path / "appointments.db"
    db.write_bytes(b"KEEP_NEW_APPOINTMENTS")
    setup = f"""set -Eeuo pipefail
ROOT={tmp_path}
CURRENT="$ROOT/current"
OLD_RELEASE="$ROOT/old"
OLD_SHA={'a'*40}
SWITCHED=1
PUBLISHER_HELD=1
HOLD_PUBLISHER={held}
PREVIOUSLY_HELD={previously_held}
PUBLISHER=qiaolian-v3@publisher.service
SERVICES=(qiaolian-v3@collector.service qiaolian-v3@canonical.service qiaolian-v3@publisher.service qiaolian-v3@user.service)
OTHER_SERVICES=(qiaolian-v3@collector.service qiaolian-v3@canonical.service qiaolian-v3@user.service)
systemctl() {{ printf '%s\\n' "$*" >> "$ROOT/service-calls"; }}
write_hold_marker() {{ printf '%s' "$1" > "$ROOT/marker"; }}
cleanup() {{ :; }}
"""
    result = subprocess.run(
        ["bash"],
        input=setup + function + "\nset +e\nfalse\nrollback_release\n",
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1, result.stderr
    assert db.read_bytes() == b"KEEP_NEW_APPOINTMENTS"
    assert (tmp_path / "current").resolve() == old
    calls = (tmp_path / "service-calls").read_text().splitlines()
    restarted_publisher = any(
        line.startswith("restart ") and "publisher.service" in line for line in calls
    )
    assert restarted_publisher is expected
    if not expected:
        assert (tmp_path / "marker").read_text() == "a" * 40


def test_real_hold_marker_records_only_boolean_switches_and_preserves_db(tmp_path):
    db = tmp_path / "test.db"
    with sqlite3.connect(db) as conn:
        conn.executescript(
            "CREATE TABLE publisher_autopilot_settings_v3(setting_key,setting_value); INSERT INTO publisher_autopilot_settings_v3 VALUES('enabled','true'); CREATE TABLE publisher_settings_v3(setting_key,setting_value); INSERT INTO publisher_settings_v3 VALUES('daily_broadcast_enabled','0'); INSERT INTO publisher_settings_v3 VALUES('marketing_broadcast_enabled','SECRET_CANARY'); CREATE TABLE appointments(id,note); INSERT INTO appointments VALUES(1,'KEEP');"
        )
    before = db.read_bytes()
    marker = tmp_path / "hold.json"
    source = remote_script()
    function = (
        "write_hold_marker() {"
        + source.split("write_hold_marker() {", 1)[1].split(
            "\nrollback_release() {", 1
        )[0]
    )
    result = subprocess.run(
        ["bash"],
        input=f"PYTHON_BOOTSTRAP={sys.executable}\nHOLD_MARKER={marker}\nDB={db}\n"
        + function
        + f'\nwrite_hold_marker {"a"*40}\n',
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    value = json.loads(marker.read_text())
    assert value["switches_unchanged"] == {
        "property": {"present": True, "enabled": True},
        "daily": {"present": True, "enabled": False},
        "marketing": {"present": True, "enabled": None},
    }
    assert marker.stat().st_mode & 0o777 == 0o600
    assert "SECRET_CANARY" not in result.stdout + marker.read_text()
    assert db.read_bytes() == before


def test_fixed_settings_only_reads_allowlist_never_sources_or_expands(monkeypatch):
    import io

    ns = audit_functions()

    def open_config(path, *args, **kwargs):
        assert str(path) == "/opt/qiaolian_v3/runtime.env"
        return io.StringIO(
            'PUBLISHER_BOT_TOKEN="HOST_ONLY_CANARY"\nCHANNEL_ID=-10012345678\nUNRELATED_SECRET=DO_NOT_READ\nCHANNEL_URL=$(touch /tmp/never)\n'
        )

    monkeypatch.setattr(Path, "open", open_config)
    values = ns["fixed_settings"]()
    assert set(values) == {"CHANNEL_ID", "PUBLISHER_BOT_TOKEN"}
    assert values["PUBLISHER_BOT_TOKEN"] == "HOST_ONLY_CANARY"


@pytest.mark.parametrize("failure", [False, True])
def test_getchat_is_readonly_and_tokens_errors_are_not_exported(monkeypatch, failure):
    import io
    import urllib.request

    ns = audit_functions()

    def get(url, **kwargs):
        assert "/getChat?" in url and "send" not in url
        if failure:
            raise RuntimeError("HOST_ONLY_CANARY " + url)
        return io.BytesIO(
            json.dumps(
                {
                    "result": {
                        "id": -10012345678,
                        "type": "channel",
                        "username": "PublicChannel",
                        "title": "公开房源频道",
                        "description": "PRIVATE_DESCRIPTION",
                    }
                }
            ).encode()
        )

    monkeypatch.setattr(urllib.request, "urlopen", get)
    result = ns["public_channel"](
        {"CHANNEL_ID": "-10012345678", "PUBLISHER_BOT_TOKEN": "HOST_ONLY_CANARY"}
    )
    output = json.dumps(result)
    assert "HOST_ONLY_CANARY" not in output and "PRIVATE_DESCRIPTION" not in output
    assert result["lookup"] == ("RuntimeError" if failure else "verified_getChat")


@pytest.fixture
def approved(tmp_path, monkeypatch):
    ns = audit_functions()
    db = tmp_path / "approved.db"
    initialize_v3_storage(db)
    media = tmp_path / "media"
    media.mkdir()
    image = media / "cover.jpg"
    image.write_bytes(b"frozen-cover")
    digest = hashlib.sha256(image.read_bytes()).hexdigest()
    real_resolve = Path.resolve

    def resolve(path, *args, **kwargs):
        if str(path) == "/opt/qiaolian_dual_bots/media":
            return media
        return real_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO canonical_records(canonical_record_id,source_post_id,schema_version,facts_json,facts_hash) VALUES('c','s','v1','{}','rev')"
    )
    conn.execute(
        "INSERT INTO listings_v3(listing_id,public_listing_id,canonical_record_id,canonical_facts_hash,canonical_facts_schema) VALUES('l','QL-RF-A2B3','c','rev','v1')"
    )
    conn.execute(
        "INSERT INTO listing_offers(offer_id,listing_id,offer_type,monthly_rent_usd,publication_policy,publishable) VALUES('o','l','rent',800,'telegram_rent',1)"
    )
    conn.execute(
        "INSERT INTO review_items(review_id,canonical_record_id,listing_id,offer_id,review_status,approved_at) VALUES('r','c','l','o','approved','2026-11-07')"
    )
    conn.execute(
        "INSERT INTO publication_packages_v3(package_id,listing_id,offer_id,canonical_record_id,package_version,status,cover_style,cover_path,post_text,actions_json,snapshot_json,frozen_file_hashes_json,public_token,canonical_facts_hash,content_hash,approved_at) VALUES('p','l','o','c',1,'approved','classic_blue',?,'富力城 2房1厅 月租800美元 QL-RF-A2B3','{}','{}',?,'public','rev','digest','2026-11-07')",
        (str(image), json.dumps({str(image): digest})),
    )
    conn.commit()
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    yield ns, conn, tables, tmp_path, image
    conn.close()


def test_public_preview_uses_global_approved_packages_even_with_empty_queue(approved):
    ns, conn, tables, root, image = approved
    assert (
        conn.execute("SELECT COUNT(*) FROM publisher_auto_items_v3").fetchone()[0] == 0
    )
    before = conn.total_changes
    result = ns["public_package"](conn, tables, root)
    assert result["eligible_approved_count"] == 1
    assert result["candidate"]["post_text"] == "富力城 2房1厅 月租800美元 QL-RF-A2B3"
    assert result["candidate"]["images"][0]["relative_path"] == "cover.jpg"
    assert conn.total_changes == before


@pytest.mark.parametrize(
    "unsafe",
    [
        "unknown",
        "sending",
        "sent",
        "committed",
        "unapproved",
        "rented",
        "old_revision",
        "private_caption",
        "changed_image",
        "escaped_image",
    ],
)
def test_unsafe_unreviewed_or_changed_packages_are_never_preview_candidates(
    approved, unsafe, tmp_path
):
    ns, conn, tables, root, image = approved
    if unsafe in ("unknown", "sending", "sent", "committed"):
        conn.execute(
            "INSERT INTO publication_delivery_attempts_v3(attempt_id,package_id,listing_id,offer_id,channel_chat_id,state) VALUES('attempt','p','l','o','-10012345678',?)",
            (unsafe,),
        )
    elif unsafe == "unapproved":
        conn.execute("UPDATE review_items SET review_status='pending'")
    elif unsafe == "rented":
        conn.execute("UPDATE listings_v3 SET inventory_status='rented'")
    elif unsafe == "old_revision":
        conn.execute("UPDATE canonical_records SET facts_hash='new'")
    elif unsafe == "private_caption":
        conn.execute(
            "UPDATE publication_packages_v3 SET post_text='请联系 +855 123456789'"
        )
    elif unsafe == "changed_image":
        image.write_bytes(b"changed")
    elif unsafe == "escaped_image":
        other = tmp_path / "private.jpg"
        other.write_bytes(b"frozen-cover")
        image.unlink()
        image.symlink_to(other)
    assert ns["public_package"](conn, tables, root)["candidate"] is None


@pytest.mark.parametrize("held,expected", [("true", 3), ("false", 4)])
def test_real_service_selection_never_restarts_publisher_while_held(held, expected):
    source = remote_script()
    selection = (
        "SERVICES=" + source.split("SERVICES=", 1)[1].split("\nSWITCHED=0", 1)[0]
    )
    result = subprocess.run(
        ["bash"],
        input=f"ROOT=/unused\nHOLD_PUBLISHER={held}\n"
        + selection
        + "\nprintf '%s\\n' \"${ACTIVE_SERVICES[@]}\"\n",
        text=True,
        capture_output=True,
        check=True,
    )
    services = result.stdout.splitlines()
    assert len(services) == expected
    assert ("qiaolian-v3@publisher.service" in services) is (held == "false")


def test_live_preflight_uses_absolute_privilege_dropper_and_omits_private_details(
    monkeypatch,
):
    ns = audit_functions()

    def run(args, **kwargs):
        assert args[0] == "/usr/sbin/runuser"
        assert kwargs["env"]["PATH"] == "/usr/bin:/bin"
        assert "--initialize" not in args and "-B" in args
        return SimpleNamespace(
            returncode=2,
            stdout=json.dumps(
                {
                    "ok": False,
                    "checks": [
                        {
                            "name": "storage:tables",
                            "ok": False,
                            "detail": "PRIVATE_CANARY",
                        }
                    ],
                }
            ),
        )

    monkeypatch.setattr(subprocess, "run", run)
    result = ns["live_preflight"](
        {"PUBLISHER_BOT_TOKEN": "HOST_ONLY_CANARY"}, Path("/opt/qiaolian_v3/current")
    )
    assert result["ok"] is False and result["exit_code"] == 2
    assert "CANARY" not in json.dumps(result)

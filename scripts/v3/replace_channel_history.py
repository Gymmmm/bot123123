#!/usr/bin/env python3
"""Persistent, newest-first replacement using the existing frozen edit chain.

Five SUCCESSFUL replacements per group. Candidate failures do not consume a
channel slot. An uncertain attempt blocks resumption instead of being retried.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from scripts.v3.reedit_channel_posts import BeforeSendNetworkError, connect, run
from v3_core.publishing.admin_bot import load_settings


def snapshot(conn, source, channel, stream_task=""):
    targets = [dict(r) for r in conn.execute("""SELECT instance_id,channel_message_id
        FROM publication_instances WHERE platform='telegram' AND publish_status='published'
        AND channel_chat_id IN (?,?) AND COALESCE(media_group_id,'')=''
        ORDER BY CAST(channel_message_id AS INTEGER) DESC,id DESC""", channel)]
    candidates = []
    seen = set()
    for row in conn.execute("""SELECT s.id,s.raw_meta_json,o.offer_id,l.listing_id
        FROM source_posts s JOIN canonical_records c ON CAST(c.source_post_id AS INTEGER)=s.id
        JOIN listings_v3 l ON l.canonical_record_id=c.canonical_record_id
        JOIN listing_offers o ON o.listing_id=l.listing_id
        WHERE s.source_name=? AND s.parse_status='parsed' AND o.offer_type='rent' AND o.offer_status='active'
        AND o.publication_policy='telegram_rent'
        AND (?='' OR s.id IN (SELECT source_id FROM history_stream_ready_v3 WHERE task_id=?))""", (source,stream_task,stream_task)):
        row = dict(row)
        anchor = int((json.loads(row["raw_meta_json"] or "{}") or {}).get("anchor_message_id") or 0)
        if anchor <= 0:
            continue  # Album ids and local insertion ids are NOT chronology.
        candidates.append({"offer_id": row["offer_id"], "listing_id": row["listing_id"],
                           "source_id": row["id"], "anchor": anchor})
    ordered = []
    for candidate in sorted(candidates, key=lambda x: (x["anchor"], x["source_id"]), reverse=True):
        if candidate["listing_id"] not in seen:
            seen.add(candidate["listing_id"])
            ordered.append(candidate)
    if not targets or (not ordered and not stream_task):
        raise ValueError("replacement_targets_or_sources_empty")
    return {"targets": targets, "sources": ordered, "target_index": 0,
            "source_index": 0, "successful": 0, "group_successful": 0,
            "status": "ready", "events": []}


def save(conn, task, state):
    conn.execute("UPDATE channel_replacement_jobs_v3 SET state_json=? WHERE task_id=?",
                 (json.dumps(state, ensure_ascii=False), task))
    conn.commit()


def recover_before_send(conn, state, run_dir):
    """Explicit recovery only for an empty pre-send report and unchanged target."""
    if state["status"] != "blocked":
        raise RuntimeError("recovery_requires_blocked_job")
    target = state["targets"][state["target_index"]]
    source = state["sources"][state["source_index"]]
    report = json.loads((Path(run_dir) / f"{target['channel_message_id']}-{source['anchor']}" / "report.json").read_text())
    if report.get("results") or report.get("stopped"):
        raise RuntimeError("recovery_requires_empty_before_send_report")
    unsettled = conn.execute("SELECT COUNT(*) FROM publication_delivery_attempts_v3 WHERE state IN ('prepared','sending','sent','unknown')").fetchone()[0]
    row = conn.execute("SELECT listing_id FROM publication_instances WHERE instance_id=?", (target["instance_id"],)).fetchone()
    if unsettled or not row or row[0] == source["listing_id"]:
        raise RuntimeError("recovery_requires_receipt_inspection")
    state["status"] = "ready"
    state["recovery_reason"] = "verified_empty_pre_send_report"


async def execute(args):
    settings = load_settings()
    from telegram import Bot
    async with Bot(settings.token) as bot:
        chat = await bot.get_chat(settings.channel_chat_id)
        channels = (str(settings.channel_chat_id), str(chat.id))
    with connect(settings.db_path) as conn:
        from v3_core.ingest.history_stream import ensure
        ensure(conn)
        conn.execute("CREATE TABLE IF NOT EXISTS channel_replacement_jobs_v3 (task_id TEXT PRIMARY KEY,state_json TEXT NOT NULL)")
        row = conn.execute("SELECT state_json FROM channel_replacement_jobs_v3 WHERE task_id=?", (args.task,)).fetchone()
        if row:
            state = json.loads(row[0])
            if getattr(args, "recover_before_send", False) and state["status"] == "blocked":
                recover_before_send(conn, state, args.run_dir)
                save(conn, args.task, state)
        else:
            state = snapshot(conn, args.source, channels, args.stream_task)
            conn.execute("INSERT INTO channel_replacement_jobs_v3 VALUES (?,?)", (args.task, json.dumps(state)))
            conn.commit()
        if state["status"] not in {"ready", "waiting", "waiting_source"}:
            raise RuntimeError("replacement_job_not_resumable:" + state["status"])
        print(json.dumps({"event": "replacement_started", "targets": len(state["targets"]),
                          "sources": len(state["sources"]), "direction": "newest_first"}), flush=True)
        while state["target_index"] < len(state["targets"]):
            if state["source_index"] >= len(state["sources"]):
                if not args.stream_task:
                    break
                fresh = snapshot(conn, args.source, channels, args.stream_task)["sources"]
                seen = {s["listing_id"] for s in state["sources"]}
                extra = [s for s in fresh if s["listing_id"] not in seen]
                if extra and state["sources"] and extra[0]["anchor"] > state["sources"][-1]["anchor"]:
                    state["status"] = "blocked_source_order"
                    save(conn, args.task, state)
                    raise RuntimeError("stream_source_order_reversed")
                state["sources"].extend(extra)
                if not extra:
                    producer = conn.execute('SELECT status FROM history_streams_v3 WHERE task_id=?',(args.stream_task,)).fetchone()
                    if producer and producer[0] == "complete":
                        break
                    state["status"] = "waiting_source"
                    save(conn, args.task, state)
                    await asyncio.sleep(3)
                    continue
            if state["group_successful"] >= 5:
                state["status"] = "waiting"
                save(conn, args.task, state)
                print(json.dumps({"event": "group_complete", "successful": state["successful"], "wait_seconds": 30}), flush=True)
                await asyncio.sleep(30)
                state["group_successful"] = 0
            target = state["targets"][state["target_index"]]
            source = state["sources"][state["source_index"]]
            state["status"] = "executing"
            save(conn, args.task, state)  # Crash at any boundary requires inspection.
            params = SimpleNamespace(execute=True, no_backup=True, source=args.source,
                after=int(target["channel_message_id"]) + 1, limit=1, newest_first=True,
                target_instance=target["instance_id"], replacement_offer=source["offer_id"],
                run_dir=str(Path(args.run_dir) / f"{target['channel_message_id']}-{source['anchor']}"))
            try:
                report = await run(params)
            except BeforeSendNetworkError:
                state["status"] = "waiting"
                state["retry_count"] = int(state.get("retry_count", 0)) + 1
                state["last_failure"] = "network_before_send"
                save(conn, args.task, state)
                delay = min(60, 5 * 2 ** min(state["retry_count"] - 1, 4))
                print(json.dumps({"event": "retry_before_send", "wait_seconds": delay}), flush=True)
                await asyncio.sleep(delay)
                continue  # Neither cursor advances; no write was attempted.
            except Exception:
                state["status"] = "blocked"
                save(conn, args.task, state)
                raise
            if not report["results"]:
                state["target_index"] += 1
                state["status"] = "ready"
                save(conn, args.task, state)
                continue
            result = report["results"][0]
            state["retry_count"] = 0
            result["source_anchor_message_id"] = source["anchor"]
            state["events"].append(result)
            if report["stopped"]:
                state["status"] = "blocked"
                save(conn, args.task, state)
                return
            if result["status"] == "updated":
                state["target_index"] += 1
                state["source_index"] += 1
                state["successful"] += 1
                state["group_successful"] += 1
            elif result.get("reason") in {"destination_channel_mismatch", "old_album_requires_manual_review"}:
                state["target_index"] += 1
            else:
                state["source_index"] += 1
            state["status"] = "ready"
            save(conn, args.task, state)
        state["status"] = "complete"
        save(conn, args.task, state)
        print(json.dumps({"event": "replacement_finished", "successful": state["successful"],
                          "targets_left": len(state["targets"]) - state["target_index"],
                          "sources_left": len(state["sources"]) - state["source_index"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="zufang555", choices=["zufang555"])
    parser.add_argument("--task", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--stream-task", default="")
    parser.add_argument("--recover-before-send", action="store_true")
    asyncio.run(execute(parser.parse_args()))

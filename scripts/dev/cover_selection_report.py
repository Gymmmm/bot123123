#!/usr/bin/env python3
"""
cover_selection_report.py
对数据库中所有有图片的 source_posts 运行 choose_best_cover_image()
输出每条房源的选图报告，不自动发布。
"""
import os
import sys
import json
import sqlite3

sys.path.insert(0, "/opt/qiaolian_dual_bots")
os.chdir("/opt/qiaolian_dual_bots")

from cover_generator import choose_best_cover_image, _score_image

DB_PATH = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 找出所有有图片的 source_posts
    rows = conn.execute("""
        SELECT sp.id, sp.source_name, sp.raw_images_json,
               d.draft_id, d.project, d.layout, d.price, d.area
        FROM source_posts sp
        LEFT JOIN drafts d ON d.source_post_id = sp.id
        WHERE sp.raw_images_json IS NOT NULL
          AND sp.raw_images_json != '[]'
          AND sp.raw_images_json != ''
        ORDER BY sp.id
    """).fetchall()
    conn.close()

    print(f"\n{'='*70}")
    print(f"选图报告 — 共 {len(rows)} 条有图片的 source_posts")
    print(f"{'='*70}\n")

    ok_count = 0
    fallback_count = 0
    no_draft_count = 0

    for row in rows:
        sp_id      = row["id"]
        source     = row["source_name"]
        draft_id   = row["draft_id"]
        project    = row["project"] or "未知楼盘"
        layout     = row["layout"] or ""
        price      = row["price"] or ""
        area       = row["area"] or ""

        try:
            images = json.loads(row["raw_images_json"])
        except Exception:
            images = []

        # 过滤有效本地路径
        valid_images = [
            p for p in images
            if isinstance(p, str)
            and p.startswith("/")
            and "dummy" not in p
            and os.path.exists(p)
        ]

        print(f"── source_post #{sp_id} | {source} | {project} {layout} {price}")
        print(f"   draft_id: {draft_id or '无关联 draft'}")
        print(f"   图片总数: {len(images)} 条记录 | 本地有效: {len(valid_images)} 张")

        if not valid_images:
            print(f"   ⚠️  无有效本地图片 → 退回默认背景\n")
            fallback_count += 1
            continue

        # 每张图打分
        scored = []
        for i, p in enumerate(valid_images):
            score, reason = _score_image(p)
            scored.append((score, i, p, reason))
        scored.sort(key=lambda x: x[0], reverse=True)

        print(f"   各图评分（共{len(valid_images)}张）：")
        for score, idx, path, reason in scored:
            fname = os.path.basename(path)
            marker = " ← 选中" if idx == scored[0][1] else ""
            print(f"     [{idx+1}] {fname}  得分={score:.1f}  {reason}{marker}")

        best_path, best_idx, selection_reason = choose_best_cover_image(valid_images)
        if best_path:
            print(f"   ✅ 最终选图: {selection_reason}")
            ok_count += 1
        else:
            print(f"   ⚠️  选图失败 → 退回默认背景: {selection_reason}")
            fallback_count += 1

        if not draft_id:
            no_draft_count += 1

        print()

    print(f"{'='*70}")
    print(f"汇总：")
    print(f"  ✅ 成功选出真实底图: {ok_count} 条")
    print(f"  ⚠️  退回默认背景:    {fallback_count} 条")
    print(f"  ℹ️  无关联 draft:    {no_draft_count} 条（source_post 有图但尚未解析）")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()

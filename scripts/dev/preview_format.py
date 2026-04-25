#!/usr/bin/env python3
"""
preview_format.py
用真实数据预览 AI 生成的 TG 发帖格式
"""
import os, json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from openai import OpenAI

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")

client = OpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
)

SYSTEM_PROMPT = """你是侨联地产（金边中文房产频道）的内容运营，负责将采集到的房源原文改写成适合 Telegram 频道发布的帖子。

风格要求：
- 面向金边华人，简体中文为主
- 语气自然、真实，像真人在推荐，不像机器人
- 重点突出价格、户型、位置这三个最关键信息
- 不夸张，不堆砌形容词

请从用户提供的房源原文中，提取信息并生成以下 JSON（不要包含 Markdown 代码块）：

{
  "title": "简洁标题，如：钻石岛香格里拉 | 2+1房 | $1500/月",
  "project": "楼盘名称",
  "community": "小区名称（如有）",
  "area": "区域，如：钻石岛、金街、BKK1等",
  "property_type": "公寓/别墅/联排/商铺",
  "price": 1500,
  "layout": "2+1房1厅",
  "size": "126平",
  "floor": "楼层（如有，否则null）",
  "deposit": "押一付一",
  "available_date": null,
  "highlights": ["亮点1", "亮点2"],
  "drawbacks": [],
  "advisor_comment": "一句话点评，口语化",
  "cost_notes": "费用备注（如有）",
  "tg_post": "完整发帖文案（见格式要求）"
}

tg_post 格式（HTML，Telegram 可渲染 <b> <i>）：

第1行：🏠 <b>[楼盘名] · [区域] 出租</b>
空行
📍 [区域/地标]
🛏 [户型]
📐 [面积]（如有）
🏢 [楼层]（如有）
💰 月租 <b>$[价格]</b>
🔑 [押金方式]（如有）
📦 [家具情况]（如有）
空行
✅ [亮点1]
✅ [亮点2]
空行
💬 [顾问一句话，口语，有温度，不超过30字]
空行
🏡 <b>侨联地产</b> · 您在金边的自己人

规则：
- tg_post 不能出现任何电话/微信/WhatsApp/联系方式
- tg_post 不超过 280 字
- price 字段只填纯数字
- 只返回 JSON，不要其他文字
"""

raw_text = """🏢 🔹香格里拉 公寓出租！🔹
户型：两房一厅 + 书房（三床）
面积：126.34平
🧳 家具家电齐全，拎包入住
💲租金：$1500/月
押金：1月
分类：#两房一厅 #公寓出租 #香格里拉 #金街 #钻石岛"""

print("正在调用 DeepSeek API 生成格式样本...\n")

response = client.chat.completions.create(
    model="deepseek/deepseek-chat",
    messages=[
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"请处理以下房源原文：\n\n{raw_text}"}
    ],
    temperature=0.3,
    max_tokens=1200,
)

content = response.choices[0].message.content.strip()
if content.startswith("```"):
    lines = content.split("\n")
    content = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

try:
    data = json.loads(content)
    print("=" * 60)
    print("【结构化字段】")
    print(f"  标题: {data.get('title')}")
    print(f"  楼盘: {data.get('project')}")
    print(f"  区域: {data.get('area')}")
    print(f"  户型: {data.get('layout')}")
    print(f"  面积: {data.get('size')}")
    print(f"  价格: ${data.get('price')}/月")
    print(f"  押金: {data.get('deposit')}")
    print(f"  亮点: {data.get('highlights')}")
    print(f"  点评: {data.get('advisor_comment')}")
    print()
    print("=" * 60)
    print("【TG 发帖文案预览】（HTML 原文）")
    print("-" * 60)
    tg_post = data.get("tg_post", "")
    print(tg_post)
    print("-" * 60)
    print()
    print("【渲染效果（去除 HTML 标签）】")
    import re
    plain = re.sub(r'<[^>]+>', '', tg_post)
    print(plain)
except json.JSONDecodeError as e:
    print(f"JSON 解析失败: {e}")
    print("原始返回：")
    print(content)

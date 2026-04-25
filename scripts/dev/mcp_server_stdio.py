
import os
import json
import sqlite3
import requests
import asyncio
from typing import Any, Optional
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# 配置
DB_PATH = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")

server = Server("qiaolian-notion-sync")

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="get_recent_listings",
            description="从远程数据库获取最近的房源记录",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 10}
                }
            }
        ),
        Tool(
            name="sync_listing_to_notion",
            description="将指定房源同步到 Notion",
            input_schema={
                "type": "object",
                "properties": {
                    "listing_id": {"type": "string"},
                    "notion_token": {"type": "string"},
                    "database_id": {"type": "string"}
                },
                "required": ["listing_id", "notion_token", "database_id"]
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    if name == "get_recent_listings":
        limit = arguments.get("limit", 10)
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM listings ORDER BY id DESC LIMIT ?", (limit,))
        rows = cursor.fetchall()
        listings = [dict(row) for row in rows]
        conn.close()
        return [TextContent(type="text", text=json.dumps(listings, ensure_ascii=False, indent=2))]

    elif name == "sync_listing_to_notion":
        listing_id = arguments["listing_id"]
        token = arguments["notion_token"]
        db_id = arguments["database_id"]
        
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM listings WHERE listing_id = ?", (listing_id,))
        listing = cursor.fetchone()
        if not listing:
            conn.close()
            return [TextContent(type="text", text=f"Error: Listing {listing_id} not found")]
        listing = dict(listing)
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        
        payload = {
            "parent": {"database_id": db_id},
            "properties": {
                "Title": {"title": [{"text": {"content": listing.get("title", "Untitled")}}]},
                "ListingID": {"rich_text": [{"text": {"content": listing_id}}]},
                "Price": {"number": listing.get("price", 0)},
                "Area": {"select": {"name": listing.get("area", "Other")}}
            }
        }
        
        resp = requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload)
        if resp.status_code != 200:
            conn.close()
            return [TextContent(type="text", text=f"Notion Error: {resp.text}")]
        
        notion_data = resp.json()
        page_id = notion_data["id"]
        
        cursor.execute("UPDATE posts SET notion_page_id = ? WHERE listing_id = ?", (page_id, listing_id))
        conn.commit()
        conn.close()
        
        return [TextContent(type="text", text=f"Success! Synced to Notion. Page ID: {page_id}")]

    return [TextContent(type="text", text="Unknown tool")]

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

if __name__ == "__main__":
    asyncio.run(main())

import re

def extract_house_info(text: str) -> dict:
    """
    智能提取房源信息（项目、户型、面积、楼层、价格、亮点）。
    针对金边中介常见的文案风格进行优化。
    """
    info = {
        "project": "",
        "layout": "",
        "area": "金边",
        "size": "",
        "floor": "",
        "price": "",
        "highlights": []
    }
    
    if not text:
        return info

    # 1. 提取价格 (匹配 $680, 680$, 680/月 等)
    price_match = re.search(r'(\$\s?\d+[\d,]*)|(\d+[\d,]*\s?\$)|(\d+[\d,]*\s?/月)', text)
    if price_match:
        info["price"] = price_match.group(0).strip()
    
    # 2. 提取面积 (匹配 45㎡, 45sqm, 45平 等)
    size_match = re.search(r'(\d+)\s?(㎡|sqm|平|平方米)', text, re.IGNORECASE)
    if size_match:
        info["size"] = f"{size_match.group(1)}㎡"
        
    # 3. 提取楼层 (匹配 8楼, 8th floor, 第8层 等)
    floor_match = re.search(r'(\d+)\s?(楼|层|floor)', text, re.IGNORECASE)
    if floor_match:
        info["floor"] = f"{floor_match.group(1)}楼"

    # 4. 提取户型 (匹配 1房1卫, 2卧2卫, Studio 等)
    layout_match = re.search(r'(\d\s?房\s?\d\s?卫)|(\d\s?卧\s?\d\s?卫)|(Studio)|(开间)', text, re.IGNORECASE)
    if layout_match:
        info["layout"] = layout_match.group(0).strip()

    # 5. 提取项目名 (通常在第一行，或包含“项目”、“公寓”字样)
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if lines:
        # 尝试从第一行提取项目名 (排除常见的标题词)
        first_line = lines[0]
        # 去掉 Emoji 和装饰符
        clean_line = re.sub(r'[^\w\s\u4e00-\u9fa5]', '', first_line).strip()
        if clean_line and len(clean_line) < 15:
            info["project"] = clean_line
    
    # 6. 提取卖点 (匹配带 ✅, •, -, * 的行)
    highlights = []
    for line in lines:
        if any(marker in line for marker in ['✅', '•', '-', '*', '✨']):
            # 去掉标记
            clean_h = re.sub(r'[✅•\-\*✨]', '', line).strip()
            if clean_h and len(clean_h) < 20:
                highlights.append(clean_h)
    info["highlights"] = highlights[:3]

    return info

if __name__ == "__main__":
    test_text = "🏠 富力城｜1房1卫\n💰 $680/月 ｜ 45㎡ ｜ 8楼\n✅ 家具基本全新\n✅ 小区泳池 / 健身房\n✅ 步行3分钟到超市"
    print(extract_house_info(test_text))

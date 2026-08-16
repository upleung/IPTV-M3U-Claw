import os
import asyncio
import aiohttp
import re
import time

# ================== 全局路径与配置 ==================
# 获取项目根目录 (根据脚本存放的相对位置向上推两级)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POOL_FILE = os.path.join(BASE_DIR, "config", "iptv-resource-pool")
FAV_FILE = os.path.join(BASE_DIR, "config", "favorite-channel")

EPG_URL = "https://epg.112114.xyz/pp.xml"
CONCURRENCY_LIMIT = 200  # 并发数（建议200-300）
TIMEOUT = 5              # 链接检测超时时间（秒）

# ================== 配置读取函数 ==================

def load_api_list():
    """从 iptv-resource-pool 中提取 URL"""
    api_list = []
    if not os.path.exists(POOL_FILE):
        print(f"[警告] 找不到资源池文件: {POOL_FILE}")
        return api_list
        
    with open(POOL_FILE, "r", encoding="utf-8") as f:
        for line in f:
            # 提取包含 http/https 的链接，忽略双引号或逗号
            match = re.search(r'(https?://[^\s",\']+)', line)
            if match:
                api_list.append(match.group(1))
    return api_list

def load_favorites_config():
    """从 favorite-channel 中提取喜爱的频道规则"""
    favorites = []
    if not os.path.exists(FAV_FILE):
        print(f"[警告] 找不到频道配置文件: {FAV_FILE}")
        return favorites

    with open(FAV_FILE, "r", encoding="utf-8") as f:
        for line in f:
            # 兼容带有注释的行和 JSON/Dict 格式
            match = re.search(r'{"group":\s*["\']([^"\']+)["\'],\s*"name":\s*["\']([^"\']+)["\'],\s*"match":\s*["\']([^"\']+)["\']}', line)
            if match:
                favorites.append({
                    "group": match.group(1),
                    "name": match.group(2),
                    "match": match.group(3)
                })
    return favorites

def standardize_group(name, original_group):
    """根据频道名称进行智能分组"""
    name_upper = name.upper()
    if "CCTV" in name_upper or "央视" in name:
        return "央视频道"
    elif "卫视" in name:
        return "卫视频道"
    elif any(x in name_upper for x in ["体育", "SPORTS", "ESPN"]):
        return "体育频道"
    elif any(x in name_upper for x in ["电影", "影院", "HBO", "剧", "CINEMA"]):
        return "影视频道"
    elif any(x in name_upper for x in ["新闻", "NEWS", "资讯"]):
        return "新闻频道"
    elif any(x in name_upper for x in ["少儿", "卡通", "儿童", "动画", "动漫"]):
        return "少儿频道"
    elif any(x in name_upper for x in ["纪实", "地理", "历史", "DISCOVERY", "NATIONAL"]):
        return "纪实频道"
    elif original_group and original_group != "未分类" and original_group != "":
        return original_group
    return "其他频道"

# ================== 异步工作流 ==================

async def fetch_m3u(session, url):
    """异步获取单个API的源内容"""
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                print(f"[成功] 获取源: {url}")
                return await response.text()
    except Exception as e:
        print(f"[失败] 无法获取源 {url}: {e}")
    return ""

async def check_url_speed(session, item):
    """异步检测可用性并记录响应延迟(测速)"""
    url = item['url']
    start_time = time.time()
    try:
        # 使用 GET 仅读取 Headers 以极大地节省带宽和提升速度
        async with session.get(url, timeout=TIMEOUT) as response:
            if response.status == 200:
                item['valid'] = True
                item['delay'] = time.time() - start_time
                return item
    except Exception:
        pass
        
    item['valid'] = False
    item['delay'] = float('inf')
    return item

def parse_m3u(m3u_text):
    """解析 M3U 并提取信息"""
    lines = m3u_text.splitlines()
    channels = []
    current_item = None

    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF"):
            tvg_name = re.search(r'tvg-name="([^"]+)"', line)
            tvg_logo = re.search(r'tvg-logo="([^"]+)"', line)
            group_title = re.search(r'group-title="([^"]+)"', line)
            
            name = line.split(',')[-1].strip()
            
            current_item = {
                'name': name,
                'tvg_name': tvg_name.group(1) if tvg_name else name,
                'tvg_logo': tvg_logo.group(1) if tvg_logo else "",
                'group_title': group_title.group(1) if group_title else "未分类"
            }
        elif line.startswith("http") and current_item:
            current_item['url'] = line
            channels.append(current_item)
            current_item = None
            
    return channels

async def main():
    start_time = time.time()
    
    # 0. 读取本地配置
    API_LIST = load_api_list()
    FAVORITES_CONFIG = load_favorites_config()
    print(f"载入配置: {len(API_LIST)} 个API源, {len(FAVORITES_CONFIG)} 个喜好频道规则。")

    # 1. 抓取所有API数据
    print(">>> 第一步：聚合所有 API 源...")
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_m3u(session, url) for url in API_LIST]
        results = await asyncio.gather(*tasks)
    
    combined_text = "\n".join(results)
    all_channels = parse_m3u(combined_text)
    
    # 初步去重（去除完全相同的URL）
    unique_urls = set()
    unique_channels = []
    for ch in all_channels:
        if ch['url'] not in unique_urls:
            unique_urls.add(ch['url'])
            unique_channels.append(ch)
            
    print(f"初步去重后剩余 {len(unique_channels)} 个流待测速...")

    # 2. 并发测速与可用性检测
    print(">>> 第二步：并发测速验证中...")
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [check_url_speed(session, ch) for ch in unique_channels]
        checked_channels = await asyncio.gather(*tasks)

    valid_channels = [ch for ch in checked_channels if ch['valid']]
    print(f"发现有效源：{len(valid_channels)} 个。开始进行测速优选和去重...")

    # 3. 测速优选 (相同频道保留速度最快的一个)
    best_channels_dict = {}
    for ch in valid_channels:
        name = ch['name']
        if name not in best_channels_dict:
            best_channels_dict[name] = ch
        else:
            # 如果当前源延迟更低，则替换
            if ch['delay'] < best_channels_dict[name]['delay']:
                best_channels_dict[name] = ch

    final_iptv_channels = list(best_channels_dict.values())
    
    # 重新构建经过智能分组的 extinf 信息
    for ch in final_iptv_channels:
        ch['group_title'] = standardize_group(ch['name'], ch['group_title'])
        ch['extinf'] = f'#EXTINF:-1 tvg-name="{ch["tvg_name"]}" tvg-logo="{ch["tvg_logo"]}" group-title="{ch["group_title"]}",{ch["name"]}'

    # 4. 输出 IPTV.m3u (优选与自动分组后)
    print(">>> 第三步：生成 IPTV.m3u (自动分组并保留最快源)...")
    iptv_path = os.path.join(BASE_DIR, "IPTV.m3u")
    with open(iptv_path, "w", encoding="utf-8") as f:
        f.write(f'#EXTM3U x-tvg-url="{EPG_URL}"\n')
        # 按分组排序一下会让m3u文件更整洁
        final_iptv_channels.sort(key=lambda x: x['group_title'])
        for ch in final_iptv_channels:
            f.write(f"{ch['extinf']}\n")
            f.write(f"{ch['url']}\n")

    # 5. 输出 Favorite.m3u (指定分组喜爱源)
    print(">>> 第四步：生成 Favorite.m3u...")
    fav_path = os.path.join(BASE_DIR, "Favorite.m3u")
    with open(fav_path, "w", encoding="utf-8") as f:
        f.write(f'#EXTM3U x-tvg-url="{EPG_URL}"\n')
        
        for fav in FAVORITES_CONFIG:
            # 从所有有效的验证池中寻找匹配项，并优先挑选速度最快的（因为 valid_channels 是包含所有有效线路的）
            candidates = [ch for ch in valid_channels if fav['match'].lower() in ch['name'].lower()]
            
            if candidates:
                # 按延迟从小到大排序，取最快的一个
                candidates.sort(key=lambda x: x['delay'])
                best_match = candidates[0]
                
                extinf = f'#EXTINF:-1 tvg-name="{fav["name"]}" tvg-logo="{best_match["tvg_logo"]}" group-title="{fav["group"]}",{fav["name"]}'
                f.write(f"{extinf}\n")
                f.write(f"{best_match['url']}\n")
                print(f"  ✔ [命中] {fav['group']} - {fav['name']} (延迟: {best_match['delay']:.2f}s)")
            else:
                print(f"  ✘ [无源] {fav['group']} - {fav['name']}")

    print(f"\n✅ 全部完成！总耗时: {time.time() - start_time:.2f} 秒")
    print(f"输出文件: IPTV.m3u ({len(final_iptv_channels)}个频道), Favorite.m3u")

if __name__ == "__main__":
    asyncio.run(main())

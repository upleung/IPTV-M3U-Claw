现代化方案，包含分类分组功能。

### 第一步：修改 GitHub Actions 工作流文件

修改 `.github/workflows/iptv.yml`，让它支持 Python 运行环境并安装所需库：

```yaml
name: IPTV Auto Update

on:
  schedule:
    - cron: "0 0 * * *"   # 每天 00:00 自动运行
  workflow_dispatch:       # 手动运行按钮

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout Repo
      uses: actions/checkout@v4

    - name: Setup Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.12'
        cache: 'pip'

    - name: Install Dependencies
      run: |
        python -m pip install --upgrade pip
        pip install aiohttp

    - name: Run IPTV Python Script
      run: |
        python scripts/streamlink_auto_match.py

    - name: Commit & Push
      run: |
        git config --global user.name "GitHub Actions"
        git config --global user.email "actions@github.com"
        git add IPTV.m3u Favorite.m3u
        git commit -m "Auto update: $(date '+%Y-%m-%d %H:%M:%S')" || echo "No changes"
        git push

```

### 第二步：编写全新的 Python 异步检测脚本

在 `scripts/` 目录下新建（或覆盖） `streamlink_auto_match.py`，并将以下代码填入。
**技术亮点**：

1. **`asyncio` + `aiohttp` 并发**：最高可同时发 200 个连接，将 30 分钟的耗时压缩至 1 分钟左右。
2. **内存聚合解析**：自动匹配 `#EXTINF` 里的 `group-title`、`tvg-logo`。
3. **双文件输出**：自动生成带标准分组格式的 `IPTV.m3u`（全部有效源）和 `Favorite.m3u`（你的专属频道）。

```python
import asyncio
import aiohttp
import re
import time

# ================== 核心配置区 ==================

API_LIST = [
    "https://gh-proxy.com/raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
    "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
    "https://cdn.qd.je/live.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/CUTV.txt",
    "https://raw.githubusercontent.com/YueChan/Live/main/GNTV.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/Global.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/Hunan.txt",
    "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/Radio.m3u",
    "https://raw.githubusercontent.com/zwc456baby/iptv_alive/refs/heads/master/live.m3u"
]

EPG_URL = "https://epg.112114.xyz/pp.xml"
CONCURRENCY_LIMIT = 200  # 并发数（数值越大越快，但不建议超过300防止被墙）
TIMEOUT = 5              # 链接检测超时时间（秒）

# 定义你需要的频道及其分组匹配规则
# 格式: {"group": "输出分组", "name": "输出名称", "match": "源文匹配关键字"}
FAVORITES_CONFIG = [
    # -------- 港澳台 --------
    {"group": "港澳台", "name": "翡翠台4K", "match": "翡翠台4K"},
    {"group": "港澳台", "name": "翡翠台", "match": "翡翠台"},
    {"group": "港澳台", "name": "翡翠台(字幕)", "match": "翡翠台(字幕)"},
    {"group": "港澳台", "name": "明珠台", "match": "明珠台"},
    {"group": "港澳台", "name": "TVB Plus", "match": "TVB Plus"},
    {"group": "港澳台", "name": "TVB剧集台", "match": "翡翠剧集台"},
    {"group": "港澳台", "name": "TVB亚洲武侠", "match": "亞洲武俠"},
    {"group": "港澳台", "name": "TVB娱乐新闻台", "match": "娛樂新聞台"},
    {"group": "港澳台", "name": "无线新闻台", "match": "无线新闻"},
    {"group": "港澳台", "name": "ViuTV", "match": "ViuTV"},
    {"group": "港澳台", "name": "Now新闻台", "match": "Now新聞台"},
    {"group": "港澳台", "name": "凤凰中文", "match": "凤凰中文"},
    {"group": "港澳台", "name": "凤凰资讯", "match": "凤凰资讯"},
    {"group": "港澳台", "name": "澳视澳门", "match": "澳视澳门"},
    {"group": "港澳台", "name": "澳门体育", "match": "澳视体育"},
    # -------- 体育台 --------
    {"group": "体育台", "name": "五星体育", "match": "五星体育"},
    {"group": "体育台", "name": "纬来体育台", "match": "纬来体育台"},
    {"group": "体育台", "name": "广东体育", "match": "广东体育"},
    {"group": "体育台", "name": "Astro AOD", "match": "AOD"},
    {"group": "体育台", "name": "Bein Sports", "match": "Bein"},
    {"group": "体育台", "name": "ESPN", "match": "ESPN"},
    # -------- CCTV --------
    {"group": "央视频道", "name": "CCTV1", "match": "CCTV1"},
    {"group": "央视频道", "name": "CCTV5", "match": "CCTV5"},
    {"group": "央视频道", "name": "CCTV5+", "match": "CCTV5+"},
    {"group": "央视频道", "name": "CCTV13", "match": "CCTV13"},
]

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

async def check_url(session, semaphore, item):
    """异步检测直播源URL是否可用"""
    url = item['url']
    async with semaphore:
        try:
            # 采用 GET 请求读取响应头即可，极大地节约带宽和时间
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status == 200:
                    item['valid'] = True
                    return item
        except Exception:
            pass
    item['valid'] = False
    return item

def parse_m3u(m3u_text):
    """解析聚合的 M3U，提取名称、URL、Logo和分组"""
    lines = m3u_text.splitlines()
    channels = []
    current_item = None

    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF"):
            # 正则提取 tvg-name, tvg-logo, group-title 和 频道名称
            tvg_name = re.search(r'tvg-name="([^"]+)"', line)
            tvg_logo = re.search(r'tvg-logo="([^"]+)"', line)
            group_title = re.search(r'group-title="([^"]+)"', line)
            
            name = line.split(',')[-1].strip()
            
            current_item = {
                'name': name,
                'tvg_name': tvg_name.group(1) if tvg_name else name,
                'tvg_logo': tvg_logo.group(1) if tvg_logo else "",
                'group_title': group_title.group(1) if group_title else "未分类",
                'raw_extinf': line
            }
        elif line.startswith("http") and current_item:
            current_item['url'] = line
            channels.append(current_item)
            current_item = None
            
    return channels

async def main():
    start_time = time.time()
    
    # 1. 抓取所有API数据
    print(">>> 第一步：聚合所有 API 源...")
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_m3u(session, url) for url in API_LIST]
        results = await asyncio.gather(*tasks)
    
    combined_text = "\n".join(results)
    all_channels = parse_m3u(combined_text)
    print(f"聚合完成，共提取到 {len(all_channels)} 个播放流待检测。")

    # 去重（根据URL去重）
    unique_urls = set()
    unique_channels = []
    for ch in all_channels:
        if ch['url'] not in unique_urls:
            unique_urls.add(ch['url'])
            unique_channels.append(ch)
            
    print(f"去重后剩余 {len(unique_channels)} 个唯一流，开始极速并发检测...")

    # 2. 并发检测所有URL
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [check_url(session, semaphore, ch) for ch in unique_channels]
        checked_channels = await asyncio.gather(*tasks)

    valid_channels = [ch for ch in checked_channels if ch['valid']]
    print(f">>> 第二步检测完成！发现有效源：{len(valid_channels)} 个。耗时: {time.time() - start_time:.2f} 秒")

    # 3. 输出 IPTV.m3u (全部有效源)
    print(">>> 第三步：生成 IPTV.m3u...")
    with open("IPTV.m3u", "w", encoding="utf-8") as f:
        f.write(f'#EXTM3U x-tvg-url="{EPG_URL}"\n')
        for ch in valid_channels:
            f.write(f"{ch['raw_extinf']}\n")
            f.write(f"{ch['url']}\n")

    # 4. 输出 Favorite.m3u (指定分组喜爱源)
    print(">>> 第四步：生成 Favorite.m3u...")
    with open("Favorite.m3u", "w", encoding="utf-8") as f:
        f.write(f'#EXTM3U x-tvg-url="{EPG_URL}"\n')
        
        # 遍历你配置的喜爱列表
        for fav in FAVORITES_CONFIG:
            # 在有效池里寻找匹配的源 (只要匹配到第一个可用的即可)
            matched = next((ch for ch in valid_channels if fav['match'].lower() in ch['name'].lower()), None)
            
            if matched:
                # 标准化组名与频道名输出
                extinf = f'#EXTINF:-1 tvg-name="{fav["name"]}" tvg-logo="{matched["tvg_logo"]}" group-title="{fav["group"]}",{fav["name"]}'
                f.write(f"{extinf}\n")
                f.write(f"{matched['url']}\n")
                print(f"  ✔ [命中] {fav['group']} - {fav['name']}")
            else:
                print(f"  ✘ [无源] {fav['group']} - {fav['name']}")

    print("✅ 全部更新完毕！")

if __name__ == "__main__":
    asyncio.run(main())

```

### 带来的显著变化：

* **速度飞跃**：原本每个链接等待 3-5 秒。现在由于启用了 `asyncio.gather()` 并发处理 200 个链接，你原本需要 **30多分钟** 的构建流程，**现在通常只需要 1 到 2 分钟**。
* **分组整洁**：所有的 M3U 文件都带上了规范的 `group-title=""`，这使你在电视盒子端（如 TiviMate / 影视仓）导入后，频道列表会自动变成你划好的“港澳台、体育台、央视频道”等清爽的菜单树。

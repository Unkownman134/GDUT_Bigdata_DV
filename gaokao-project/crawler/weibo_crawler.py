# -*- coding: utf-8 -*-
"""
微博高考话题爬虫 (多线程版)
s.weibo.com → 帖子全文/用户/时间/互动/话题/ID → Kafka
ThreadPoolExecutor 并行爬取, 配置见 config.py
"""
import json, time, re, threading
import requests
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from kafka import KafkaProducer

import sys, os, json
with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json"), "r", encoding="utf-8") as _f:
    CFG = json.load(_f)

KEYWORDS = CFG["crawler"]["keywords"]

PRODUCER = KafkaProducer(
    bootstrap_servers=CFG["kafka"]["bootstrap_servers"],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    compression_type="gzip",
)

_COOKIE_DICT = CFG["weibo_cookie"]
FULL_COOKIE = "; ".join(f"{k}={v}" for k, v in _COOKIE_DICT.items() if k != "XSRF-TOKEN")

HEADERS_TEMPLATE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Cookie": FULL_COOKIE,
}

# 线程安全的去重集合
_SEEN_LOCK = threading.Lock()

# 爬虫端快速预筛排除词
_CRAWLER_EXCLUDE = [
    "亲子鉴定", "巢湖", "征婚", "上门按摩", "代孕", "试管婴儿",
    "抽奖", "限时抢", "立即抢购", "一件代发", "货源", "招代理",
    "兼职日结", "月入过万", "点击链接", "领券", "薅羊毛",
    "追星打榜", "耽美", "同人文", "夏日童趣", "知识挑战赛",
    "同城交友", "找对象", "相亲", "附近的人",
    "整容", "隆鼻", "双眼皮", "瘦脸", "植发",
    "棋牌", "时时彩", "六合彩", "赌博", "稳赚",
    "恐怖故事", "灵异", "风水", "算命", "星座运势",
]
# 注: "秒杀"保留不排除（可能是秒杀试卷/秒杀题目）
SEEN_MIDS: set = set()

# ===== 请求限速器 =====
class _RateLimiter:
    """请求速率控制: 防止瞬时请求风暴，不限制微博反爬速率"""
    def __init__(self, max_qps=30):
        self.min_interval = 1.0 / max_qps
        self.last_time = 0
        self.lock = threading.Lock()

    def wait(self):
        import random
        with self.lock:
            elapsed = time.time() - self.last_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            # 最小抖动 0~0.05s
            time.sleep(random.random() * 0.05)
            self.last_time = time.time()


_RATE_LIMITER = _RateLimiter(max_qps=30)  # 高并发模式，快速耗尽配额以尽早触发冷却计时

# 用户 Profile 缓存 (uid→{gender,location})
_PROFILE_CACHE: dict = {}
_PROFILE_LOCK = threading.Lock()

# 用于 profile API 的独立 Session（避免 cookie 冲突）
_PROFILE_SESSION = requests.Session()
_PROFILE_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://weibo.com/",
    "Cookie": FULL_COOKIE,
    "X-XSRF-TOKEN": _COOKIE_DICT.get("XSRF-TOKEN", ""),
})


def fetch_profile(uid: str) -> dict:
    """调用 weibo.com/ajax/profile/info 获取性别+地区，带缓存"""
    if not uid:
        return {"gender": "", "location": ""}
    with _PROFILE_LOCK:
        if uid in _PROFILE_CACHE:
            return _PROFILE_CACHE[uid]
    try:
        r = _PROFILE_SESSION.get(
            f"https://weibo.com/ajax/profile/info?uid={uid}", timeout=10
        )
        if r.status_code == 200:
            d = r.json()
            if d.get("ok") == 1:
                u = d.get("data", {}).get("user", {})
                result = {
                    "gender": u.get("gender", ""),
                    "location": u.get("location", ""),
                }
                with _PROFILE_LOCK:
                    _PROFILE_CACHE[uid] = result
                return result
    except Exception:
        pass
    return {"gender": "", "location": ""}


# 每个线程独立 Session（避免连接冲突）
_thread_sessions = threading.local()


def _get_session():
    if not hasattr(_thread_sessions, "session"):
        s = requests.Session()
        s.headers.update(HEADERS_TEMPLATE)
        _thread_sessions.session = s
    return _thread_sessions.session


def search_one(keyword: str, page: int) -> list[dict]:
    """单个关键词分页搜索: 从 s.weibo.com 获取帖子列表，遇到 418 限流直接跳过"""
    import random
    url = f"https://s.weibo.com/weibo?q={quote(keyword)}&typeall=1&suball=1&sort=time&page={page}"
    session = _get_session()

    for attempt in range(3):  # 最多重试3次
        _RATE_LIMITER.wait()  # 限速
        try:
            r = session.get(url, timeout=(10, 20))
        except Exception as e:
            print(f"  [警告] [{keyword}] p{page} 请求异常: {e}", flush=True)
            time.sleep(2 ** attempt)  # 指数退避
            continue

        if r.status_code == 200:
            break  # 成功
        elif r.status_code == 418:
            print(f"  [限流] [{keyword}] p{page} HTTP 418，跳过（不计入评分）", flush=True)
            return []
        else:
            print(f"  [警告] [{keyword}] p{page} HTTP {r.status_code}", flush=True)
            return []
    else:
        # 重试耗尽（非418）
        return []
    # 请求成功，解析页面
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        posts = []
        for card in soup.select(".card-wrap"):
            txt_node = card.select_one(".txt")
            if not txt_node:
                continue
            for rt in txt_node.select(".retweet"):
                rt.decompose()
            text = txt_node.get_text(strip=True)
            if len(text) < 6:
                continue

            mid = card.get("mid", "")
            if not mid:
                continue

            user_node = card.select_one(".name")
            user = user_node.get_text(strip=True) if user_node else "?"
            # 提取 uid 并获取用户画像（性别+地区）
            uid = ""
            if user_node:
                m = re.search(r'weibo\.com/(\d+)', user_node.get("href", ""))
                if m:
                    uid = m.group(1)
            profile = fetch_profile(uid)
            gender = profile.get("gender", "")
            location = profile.get("location", "")

            tm = card.select_one(".from a")
            wb_time = tm.get_text(strip=True) if tm else ""

            # mid 去重
            with _SEEN_LOCK:
                if mid in SEEN_MIDS:
                    continue
                SEEN_MIDS.add(mid)

            topics = re.findall(r"#([^#]+)#", text)
            posts.append({
                "text": text,
                "keyword": keyword,
                "user_name": user,
                "gender": gender,
                "location": location,
                "wb_time": wb_time,
                "topics": topics,
                "mid": mid,
                "send_time": int(time.time()),
            })
        return posts
    except Exception as e:
        print(f"  [警告] [{keyword} p{page}] 解析异常: {e}", flush=True)
        return []


# ===== 健康检测 =====
def health_check() -> bool:
    """验证微博 Cookie/Session 是否有效，搜索必然有结果的关键词进行探测"""
    import random
    try:
        _RATE_LIMITER.wait()
        session = _get_session()
        # "高考" 必定有结果，只爬第1页，看能否正常返回帖子
        r = session.get(
            "https://s.weibo.com/weibo?q=%E9%AB%98%E8%80%83&typeall=1&suball=1&sort=time",
            timeout=(10, 20),
        )
        if r.status_code != 200:
            print(f"  [失败] 健康检测失败! HTTP {r.status_code}", flush=True)
            if r.status_code == 418:
                print(f"  [提示] 微博返回 418（限流），Cookie 可能已过期或触发反爬策略。", flush=True)
                print(f"  [操作] 请打开浏览器访问 weibo.com，登录后按 F12 -> 网络 -> 复制请求头中的 Cookie", flush=True)
                print(f"  [操作] 粘贴到 config.json 的 weibo_cookie 字段，然后重启爬虫", flush=True)
            return False
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".card-wrap")
        if not cards:
            # 可能是反爬或Cookie过期——检查页面特征
            if "passport" in r.url or "login" in r.text[:2000]:
                print("  [失败] Cookie 已过期! 请更新 config.json 中的 weibo_cookie", flush=True)
            else:
                print("  [失败] 搜索结果为空! 可能被微博反爬限制", flush=True)
            return False
        print(f"  [正常] 健康检测通过（搜索'高考': {len(cards)} 条）", flush=True)
        return True
    except Exception as e:
        print(f"  [失败] 健康检测异常: {e}", flush=True)
        return False


# ===== 关键词有效性追踪 =====
_KEYWORD_STATS: dict = {}  # keyword -> {"rounds":0, "total":0, "zeros":0}
_KEYWORD_STATS_LOCK = threading.Lock()

# ===== 关键词综合评分系统 =====
_KEYWORD_METRICS: dict = {}  # keyword -> {"avg":0.0, "good_streak":0, "bad_streak":0}
_KEYWORD_ROUND_POSTS: dict = {}  # keyword -> 本轮帖子数
_KEYWORD_METRICS_LOCK = threading.Lock()


def track_keyword(kw: str, count: int):
    with _KEYWORD_STATS_LOCK:
        if kw not in _KEYWORD_STATS:
            _KEYWORD_STATS[kw] = {"rounds": 0, "total": 0, "zeros": 0}
        _KEYWORD_STATS[kw]["rounds"] += 1
        _KEYWORD_STATS[kw]["total"] += count
        if count == 0:
            _KEYWORD_STATS[kw]["zeros"] += 1


def print_keyword_report():
    """输出关键词有效性报告"""
    with _KEYWORD_STATS_LOCK:
        if not _KEYWORD_STATS:
            return
        dead = [(kw, s) for kw, s in _KEYWORD_STATS.items()
                if s["rounds"] >= 3 and s["total"] == 0]
        weak = [(kw, s) for kw, s in _KEYWORD_STATS.items()
                if s["rounds"] >= 3 and s["total"] > 0 and s["zeros"] / s["rounds"] > 0.7]
        if dead:
            print(f"  [警告] 以下关键词连续多轮无结果，建议删除:")
            for kw, s in dead[:10]:
                print(f"     - {kw} ({s['rounds']} 轮)")
        if weak:
            print(f"  [警告] 以下关键词命中率低（>70% 为空）:")
            for kw, s in weak[:10]:
                rate = int(s["zeros"] / s["rounds"] * 100)
                print(f"     - {kw} (空率 {rate}%，累计 {s['total']} 条)")


def main():
    pages = CFG["crawler"]["pages"]
    workers = CFG["crawler"]["workers"]
    interval = CFG["crawler"]["interval_seconds"]
    kw_count = len(KEYWORDS)
    # sort=time 时，后续轮次只需爬第1页（后面的页都是重复的）
    use_sort_time = True  # 当前固定使用 sort=time
    incremental_pages = 2 if use_sort_time else pages

    # 加载持久化的关键词评分
    for kw, m in CFG["crawler"].get("keyword_metrics", {}).items():
        _KEYWORD_METRICS[kw] = m
    top3 = sorted(KEYWORDS, key=lambda kw: _KEYWORD_METRICS.get(kw, {}).get("avg", 0), reverse=True)[:3]

    print("=" * 55)
    print("  微博高考话题爬虫（多线程版）")
    print(f"  关键词数: {kw_count} | 排序: 时间")
    print(f"  首轮页数: {pages} （首轮）/ {incremental_pages} （增量）| 线程数: {workers} | 间隔: {interval}s")
    print(f'  [评分] 热门关键词: {", ".join(top3)}')
    print("=" * 55, flush=True)

    # 首次健康检测
    if not health_check():
        print("  [失败] 健康检测未通过，等待 60 秒后重试...", flush=True)
        time.sleep(60)

    round_num = 0
    while True:
        round_num += 1
        total = 0

        # 首轮: 全部页面; 后续轮次: 增量（仅最新页面）
        page_count = pages if round_num == 1 else incremental_pages
        # 按综合评分排序: 历史产出高且稳定的关键词优先
        def _score(kw):
            m = _KEYWORD_METRICS.get(kw, {})
            return m.get("avg", 0) * 2 + m.get("good_streak", 0) - m.get("bad_streak", 0) * 3
        sorted_kws = sorted(KEYWORDS, key=_score, reverse=True)
        tasks = [(kw, page) for kw in sorted_kws for page in range(1, page_count + 1)]
        _KEYWORD_ROUND_POSTS.clear()

        start = time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(search_one, kw, page): (kw, page) for kw, page in tasks}

            for future in as_completed(futures):
                kw, page = futures[future]
                posts = future.result()
                count = len(posts)
                total += count
                track_keyword(kw, count)
                with _KEYWORD_METRICS_LOCK:
                    _KEYWORD_ROUND_POSTS[kw] = _KEYWORD_ROUND_POSTS.get(kw, 0) + count
                marker = " [新]" if count > 0 else ""
                print(f"  [{kw}] p{page} -> {count} 帖{marker}", flush=True)

                for p in posts:
                    # 爬虫端快速预筛: 排除明显无关/广告内容
                    if any(t in p["text"] for t in _CRAWLER_EXCLUDE):
                        continue
                    PRODUCER.send(CFG["kafka"]["topic"], value={
                        "type": "danmaku",
                        "text": p["text"],
                        "keyword": kw,
                        "source": "weibo",
                        "user_name": p.get("user_name", ""),
                        "gender": p.get("gender", ""),
                        "location": p.get("location", ""),
                        "topics": p.get("topics", []),
                        "mid": p.get("mid", ""),
                        "wb_time": p.get("wb_time", ""),
                        "send_time": p["send_time"],
                    })

        PRODUCER.flush()
        elapsed = time.time() - start
        with _SEEN_LOCK:
            unique = len(SEEN_MIDS)
        print(f"  [正常] 第 {round_num:>3} 轮: +{total} 帖 | {elapsed:.0f}s | 累计: {unique}", flush=True)

        # ===== 更新综合评分 =====
        changed = []
        with _KEYWORD_METRICS_LOCK:
            for kw in KEYWORDS:
                m = _KEYWORD_METRICS.setdefault(kw, {"avg": 0.0, "good_streak": 0, "bad_streak": 0})
                round_posts = _KEYWORD_ROUND_POSTS.get(kw, 0)
                old_avg = m["avg"]

                # 指数滑动平均（近期数据权重更高）
                m["avg"] = m["avg"] * 0.7 + round_posts * 0.3

                if round_posts > 0:
                    m["good_streak"] += 1
                    m["bad_streak"] = 0
                    if m["good_streak"] == 1:
                        changed.append((kw, "[回暖]", f"滑动平均: {old_avg:.0f} -> {m['avg']:.0f}"))
                else:
                    m["good_streak"] = 0
                    m["bad_streak"] += 1
                    if m["bad_streak"] >= 3 and m["bad_streak"] <= 4:
                        changed.append((kw, f"[持续空结果 {m['bad_streak']}轮]", f"滑动平均: {old_avg:.0f} -> {m['avg']:.0f}"))

            # 展示评分前5名
            scored = sorted(
                [(kw, m["avg"] * 2 + m["good_streak"] - m["bad_streak"] * 3) for kw, m in _KEYWORD_METRICS.items()],
                key=lambda x: -x[1]
            )[:5]
            if scored:
                parts = [f"{kw}({s:.0f})" for kw, s in scored]
                print(f'  [评分] TOP5: {" | ".join(parts)}', flush=True)

        if changed:
            for kw, tag, detail in changed[:3]:
                print(f"  {tag} [{kw}] {detail}", flush=True)

        # 持久化评分到 config.json
        try:
            CFG["crawler"]["keyword_metrics"] = {kw: dict(m) for kw, m in _KEYWORD_METRICS.items()}
            CFG["crawler"].pop("keyword_priority", None)  # 清理旧字段
            _cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
            with open(_cfg_path, "w", encoding="utf-8") as _f:
                json.dump(CFG, _f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        # 定时健康检测 + 关键词报告（每3轮）
        if round_num % 3 == 0:
            print(f"  [健康检测] 执行定时健康检测...", flush=True)
            if not health_check():
                print("  [警告] 健康检测失败，继续运行...", flush=True)
            print_keyword_report()

        print(f"  [等待] {interval}s...\n", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()

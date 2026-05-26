# -*- coding: utf-8 -*-
"""
Gaokao Topic Real-time Monitor v5
聚焦: 帖子正文分析 + 发帖量 + 关键词 + 情感 + 用户分析
"""
import streamlit as st
import streamlit.components.v1 as components
import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys, os, json, re, uuid

# 加载配置
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as _f:
    CFG = json.load(_f)
MYSQL = CFG["mysql"]

st.set_page_config(page_title="高考监控中心", layout="wide", initial_sidebar_state="collapsed")

# 全局时间范围选项
TIME_RANGES = {
    "15分钟": 15,
    "30分钟": 30,
    "1小时": 60,
    "2小时": 120,
    "4小时": 240,
    "12小时": 720,
    "24小时": 1440,
    "所有": 0,  # 0 表示不限制时间范围
}

def get_time_condition(hours):
    """生成时间条件 SQL"""
    if hours == 0:
        return "1=1"  # 不限制时间范围
    return f"window_time >= NOW() - INTERVAL {hours} MINUTE"

# ========== 各图表独立数据加载函数 ==========

@st.cache_data(ttl=15)
def _load_kpi_health():
    """加载 KPI 卡片 + 健康检测数据（全局，不按时间筛选）"""
    conn = mysql.connector.connect(**MYSQL)
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(video_count),0) FROM video_stats"); total_posts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT keyword) FROM video_stats"); topics = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT user_name) FROM raw_posts"); users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM keyword_ranking WHERE count>0"); akw = cur.fetchone()[0]
    d_day = (datetime(2026,6,7)-datetime.now()).days
    
    try:
        cur.execute("SELECT MAX(window_time) FROM raw_posts")
        last_post = cur.fetchone()[0]
    except: last_post = None
    try:
        cur.execute("SELECT COUNT(*) FROM raw_posts WHERE window_time>=NOW()-INTERVAL 1 MINUTE")
        posts_1m = cur.fetchone()[0]
    except: posts_1m = 0
    try:
        cur.execute("SELECT keyword, COUNT(*) as cnt FROM raw_posts WHERE window_time >= NOW()-INTERVAL 5 MINUTE GROUP BY keyword ORDER BY cnt DESC")
        kw_health = cur.fetchall()
    except: kw_health = []
    try:
        cur.execute("SELECT COUNT(*) FROM raw_posts")
        total_raw = cur.fetchone()[0]
    except: total_raw = 0
    
    # 关键词总量
    try:
        df_k = pd.read_sql("SELECT keyword,count FROM keyword_ranking ORDER BY count DESC LIMIT 30", conn)
    except: df_k = pd.DataFrame()
    
    conn.close()
    return total_posts, topics, users, akw, d_day, last_post, posts_1m, kw_health, total_raw, df_k

@st.cache_data(ttl=15)
def _query_video_stats(time_hours):
    """热度趋势数据"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    try:
        df = pd.read_sql(f"SELECT window_time,keyword,video_count FROM video_stats WHERE {time_cond} ORDER BY window_time", conn)
        if not df.empty: df["window_time"] = pd.to_datetime(df["window_time"])
    except: df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=15)
def _query_danmaku(time_hours):
    """实时速率数据"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    try:
        df = pd.read_sql(f"SELECT window_time,SUM(count) as total FROM danmaku_per_minute WHERE {time_cond} GROUP BY window_time ORDER BY window_time", conn)
        if not df.empty: df["window_time"] = pd.to_datetime(df["window_time"])
    except: df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=15)
def _query_sentiment_ts(time_hours):
    """情感时序数据"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    try:
        df = pd.read_sql(f"SELECT window_time,positive,neutral,negative FROM sentiment_per_minute WHERE {time_cond} ORDER BY window_time DESC LIMIT 120", conn)
        if not df.empty: df["window_time"] = pd.to_datetime(df["window_time"])
    except: df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=15)
def _query_sentiment_pie(time_hours):
    """情感分布饼图数据（用于无选择器的情感仪表+分布）"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    try:
        df = pd.read_sql(f"SELECT sentiment, COUNT(*) as cnt FROM raw_posts WHERE sentiment IN ('positive','neutral','negative') AND {time_cond} GROUP BY sentiment", conn)
    except: df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=15)
def _query_latest_posts(time_hours):
    """最新帖子数据"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    try:
        df = pd.read_sql(f"SELECT keyword,user_name,gender,location,sentiment,text_preview,window_time FROM raw_posts WHERE {time_cond} ORDER BY id DESC LIMIT 100", conn)
        if not df.empty: df["window_time"] = pd.to_datetime(df["window_time"])
    except: df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=15)
def _query_profile(time_hours):
    """用户画像数据（性别+地区+活跃用户）"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    try:
        df_gender = pd.read_sql(f"SELECT gender, COUNT(*) as cnt FROM raw_posts WHERE gender IN ('m','f') AND {time_cond} GROUP BY gender", conn)
    except: df_gender = pd.DataFrame()
    try:
        df_loc = pd.read_sql(f"SELECT location, COUNT(*) as cnt FROM raw_posts WHERE location!='' AND location NOT IN ('其他','其它') AND {time_cond} GROUP BY location ORDER BY cnt DESC LIMIT 20", conn)
    except: df_loc = pd.DataFrame()
    try:
        df_user = pd.read_sql(f"SELECT user_name, COUNT(*) as cnt FROM raw_posts WHERE user_name!='' AND user_name!='?' AND {time_cond} GROUP BY user_name ORDER BY cnt DESC LIMIT 15", conn)
    except: df_user = pd.DataFrame()
    conn.close()
    return df_gender, df_loc, df_user

@st.cache_data(ttl=15)
def _query_cross_analysis(time_hours):
    """深度交叉分析数据（帖子长度+情感热力图+关键词趋势）"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    try:
        df_len = pd.read_sql(f"SELECT LENGTH(text_preview) as text_len FROM raw_posts WHERE LENGTH(text_preview) BETWEEN 1 AND 300 AND {time_cond} ORDER BY id DESC LIMIT 2000", conn)
    except: df_len = pd.DataFrame()
    try:
        df_heat = pd.read_sql(f"SELECT r.keyword, r.sentiment, COUNT(*) as cnt FROM raw_posts r INNER JOIN (SELECT keyword FROM keyword_ranking ORDER BY count DESC LIMIT 10) k ON r.keyword = k.keyword WHERE r.sentiment IN ('positive','neutral','negative') AND {time_cond} GROUP BY r.keyword, r.sentiment", conn)
    except: df_heat = pd.DataFrame()
    try:
        df_trend = pd.read_sql(f"SELECT window_time, SUM(video_count) as cumulative FROM video_stats WHERE {time_cond} GROUP BY window_time ORDER BY window_time", conn)
        if not df_trend.empty:
            df_trend["window_time"] = pd.to_datetime(df_trend["window_time"])
            df_trend["cumulative"] = df_trend["cumulative"].cumsum()
    except: df_trend = pd.DataFrame()
    conn.close()
    return df_len, df_heat, df_trend

@st.cache_data(ttl=15)
def _query_more_analysis(time_hours):
    """更多分析维度数据（地域x情感+性别x情感+时段分布）"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    try:
        df = pd.read_sql(f"SELECT keyword,user_name,gender,location,sentiment,text_preview,window_time FROM raw_posts WHERE {time_cond} ORDER BY id DESC LIMIT 3000", conn)
        if not df.empty: df["window_time"] = pd.to_datetime(df["window_time"])
    except: df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=15)
def _query_hourly_dist(time_hours):
    """小时发帖分布"""
    df = _query_video_stats(time_hours)
    if not df.empty:
        df["hour"] = df["window_time"].dt.hour
        df = df.groupby("hour")["video_count"].sum().reset_index()
    return df

def clean_text(text):
    """清理文本中的 emoji 和特殊字符"""
    if not text:
        return ""
    # 移除 emoji
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642"
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # variation selectors-16
        u"\u3030"
                      "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)
    # 移除多余空格和换行
    text = ' '.join(text.split())
    return text.strip()

@st.cache_data(ttl=30)
def load_wordcloud(time_hours=120):
    """生成关键词词云（支持时间范围）"""
    from wordcloud import WordCloud
    import io
    
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    # keyword_ranking 表使用 update_time 而非 window_time
    time_cond = time_cond.replace("window_time", "update_time")
    
    try:
        df = pd.read_sql(
            f"SELECT keyword,count FROM keyword_ranking WHERE {time_cond} ORDER BY count DESC LIMIT 100",
            conn
        )
    except:
        df = pd.DataFrame()
    conn.close()

    if df.empty:
        return None

    freq = dict(zip(df["keyword"].fillna(""), df["count"].fillna(0).astype(int)))
    freq = {k: v for k, v in freq.items() if k and v > 0 and len(k) >= 1}
    if not freq:
        return None

    # 找系统可用的中文字体（优先Linux字体）
    _font_candidates = [
        r"/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",  # Linux - 文泉驿微米黑
        r"/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",   # Linux - 文泉驿正黑
        r"/usr/share/fonts/truetype/arphic/ukai.ttc",      # Linux - 文鼎粗黑
        r"/usr/share/fonts/truetype/arphic/uming.ttc",     # Linux - 文鼎明体
        r"/usr/share/fonts/truetype/noto/NotoSansCJK-SC.ttc",  # Linux - Noto Sans
        r"/usr/share/fonts/truetype/noto/NotoSansCJK-TC.ttc",  # Linux - Noto Sans TC
        r"/usr/share/fonts/truetype/noto/NotoSansCJK-JP.ttc",  # Linux - Noto Sans JP
        r"/usr/share/fonts/truetype/fonts-beng/noto-sans-bengali-ui.ttf",
        r"/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        r"/usr/share/fonts/chinese/simsun.ttf",            # Linux - 宋体
        r"/usr/share/fonts/chinese/msyh.ttc",              # Linux - 微软雅黑
        r"/usr/share/fonts/windows/msyh.ttc",              # Linux - Windows字体
        r"/usr/local/share/fonts/wqy-microhei.ttc",        # 自定义安装路径
        r"/opt/fonts/wqy-microhei.ttc",                    # 自定义安装路径
        r"/System/Library/Fonts/PingFang.ttc",             # macOS
        r"/Library/Fonts/PingFang.ttc",                    # macOS
        r"C:\Windows\Fonts\msyh.ttc",                      # Windows
        r"C:\Windows\Fonts\simhei.ttf",                    # Windows
        r"C:\Windows\Fonts\simsun.ttc",                    # Windows - 宋体
    ]
    
    _fp = None
    for p in _font_candidates:
        if os.path.exists(p):
            _fp = p
            break
    
    # 如果找不到字体，尝试使用默认字体（可能不支持中文）
    if _fp is None:
        import warnings
        warnings.warn("未找到中文字体，词云可能显示方框")
    
    wc = WordCloud(
        width=800, height=350,
        background_color=None, mode="RGBA",
        font_path=_fp,
        max_words=80, colormap="viridis",
        prefer_horizontal=0.7,
        min_font_size=10, max_font_size=80,
        collocations=False,
        regexp=r"[\u4e00-\u9fa5a-zA-Z0-9]+",
    ).generate_from_frequencies(freq)

    buf = io.BytesIO()
    wc.to_image().save(buf, format="PNG")
    return buf.getvalue()

@st.cache_data(ttl=60)
def load_topics(time_hours=120):
    """话题聚类：从 raw_posts 提取文本 -&gt; TF-IDF + KMeans -&gt; 话题分组"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    
    try:
        df = pd.read_sql(
            f"SELECT text_preview FROM raw_posts "
            f"WHERE LENGTH(text_preview)>=6 AND text_preview!='' AND {time_cond} "
            "ORDER BY id DESC LIMIT 3000",
            conn
        )
    except:
        df = pd.DataFrame()
    conn.close()

    if df.empty or len(df) < 10:
        return pd.DataFrame(), pd.DataFrame()

    _streaming_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "streaming")
    if _streaming_dir not in sys.path:
        sys.path.insert(0, _streaming_dir)
    from ai_filter import train_topic_model, predict_topic

    texts = df["text_preview"].tolist()
    train_topic_model(texts, n_topics=6)
    df["topic"] = df["text_preview"].apply(predict_topic)

    topic_counts = df["topic"].value_counts().reset_index()
    topic_counts.columns = ["topic", "count"]
    topic_counts = topic_counts[topic_counts["topic"] != "未分类"].head(10)

    topic_samples = df[df["topic"] != "未分类"].groupby("topic")["text_preview"] \
        .apply(lambda x: x.head(5).tolist()).reset_index()

    return topic_counts, topic_samples

# ========== 访客追踪 ==========
def _init_visitor():
    """初始化访客追踪，返回 (页面访问次数, 当前在线)"""
    conn = mysql.connector.connect(**MYSQL)
    cur = conn.cursor()
    # 总访问计数器（单行累计）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS visitor_count (
            id INT PRIMARY KEY DEFAULT 1,
            total INT DEFAULT 0
        )
    """)
    # 在线会话表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS visitor_sessions (
            id VARCHAR(64) PRIMARY KEY,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # 新会话 → 总计数 +1
    if "visitor_done" not in st.session_state:
        st.session_state.visitor_done = True
        cur.execute("INSERT INTO visitor_count (id, total) VALUES (1, 1) ON DUPLICATE KEY UPDATE total = total + 1")
        conn.commit()
    
    # 会话心跳
    if "visitor_sid" not in st.session_state:
        st.session_state.visitor_sid = str(uuid.uuid4())
        try:
            cur.execute("INSERT INTO visitor_sessions (id) VALUES (%s)", (st.session_state.visitor_sid,))
            conn.commit()
        except:
            pass
    
    # 读取统计
    cur.execute("SELECT total FROM visitor_count WHERE id = 1")
    row = cur.fetchone()
    total = row[0] if row else 0
    # 1分钟内有心跳的算在线，避免刷新后旧会话残留
    cur.execute("SELECT COUNT(*) FROM visitor_sessions WHERE last_seen >= NOW() - INTERVAL 1 MINUTE")
    online = cur.fetchone()[0]
    conn.close()
    return total, online

# ========== 关键词共现网络 ==========
@st.cache_data(ttl=30)
def load_keyword_network():
    """关键词共现网络：统计 TOP20 关键词在同一条帖子中出现的次数"""
    import networkx as nx
    from collections import defaultdict
    import itertools
    
    conn = mysql.connector.connect(**MYSQL)
    try:
        top_kw = pd.read_sql("SELECT keyword FROM keyword_ranking ORDER BY count DESC LIMIT 20", conn)
        keywords = top_kw["keyword"].tolist()
    except:
        keywords = []
    try:
        df = pd.read_sql("SELECT text_preview FROM raw_posts ORDER BY id DESC LIMIT 3000", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    
    if not keywords or df.empty:
        return None, None, None
    
    # 统计共现次数
    co_occur = defaultdict(float)
    kw_set = set(keywords)
    for text in df["text_preview"].dropna():
        found = [kw for kw in keywords if kw in str(text)]
        for a, b in itertools.combinations(found, 2):
            k1, k2 = (a, b) if a < b else (b, a)
            co_occur[(k1, k2)] += 1
    
    if not co_occur:
        return None, None, None
    
    # 构建图
    G = nx.Graph()
    max_w = max(co_occur.values())
    # 过滤弱关联（只保留共现 >= 2 次或共现次数达到最大值的 20% 以上）
    threshold = max(2, int(max_w * 0.2))
    for (kw1, kw2), w in co_occur.items():
        if w >= threshold:
            G.add_edge(kw1, kw2, weight=w)
    
    # 节点大小 = 关键词热度
    conn2 = mysql.connector.connect(**MYSQL)
    try:
        kw_counts = pd.read_sql("SELECT keyword, count FROM keyword_ranking WHERE keyword IN ({})".format(
            ','.join(["'"+k+"'" for k in keywords])), conn2)
        count_dict = dict(zip(kw_counts["keyword"], kw_counts["count"]))
    except:
        count_dict = {}
    conn2.close()
    
    return G, keywords, count_dict

def main():
    # ===== 主题系统 =====
    THEMES = {
        "深空黑": {
            "bg": "#0d1117", "card_bg": "linear-gradient(135deg,#1a1a2e,#16213e)",
            "card_border": "#2a2a4a", "text": "#e0e0e0", "text2": "#aaa",
            "accent": "#ff6b35", "label": "#8b8b9e",
        },
        "极光蓝": {
            "bg": "#0a1628", "card_bg": "linear-gradient(135deg,#0f2647,#1a365d)",
            "card_border": "#2a4a7a", "text": "#d0e0f0", "text2": "#8ab4f8",
            "accent": "#42a5f5", "label": "#78909c",
        },
        "烈焰橙": {
            "bg": "#1a0f0a", "card_bg": "linear-gradient(135deg,#2a1a0f,#3d2212)",
            "card_border": "#5a3520", "text": "#f0e0d0", "text2": "#d4a574",
            "accent": "#ff8a3d", "label": "#9e7a5a",
        },
        "薄荷绿": {
            "bg": "#0a1a12", "card_bg": "linear-gradient(135deg,#0f2a1e,#1a3d2e)",
            "card_border": "#2a5a40", "text": "#d0f0e0", "text2": "#80c0a0",
            "accent": "#4caf50", "label": "#6a9e7a",
        },

    }
    _theme_name = st.sidebar.selectbox("主题", list(THEMES.keys()), index=0, key="theme_selector")
    T = THEMES[_theme_name]
    _fc = "#ccc"  # 图表字体颜色
    
    # 字体颜色随主题
    _tc = T["text"]
    _tc_header = _tc
    _tc_status = _tc
    _st_info_color = _tc
    
    st.markdown(f"""<style>
    .stApp{{background:{T["bg"]}}}.metric-card{{background:{T["card_bg"]};border-radius:12px;padding:16px;text-align:center;border:1px solid {T["card_border"]}}}
    .metric-value{{font-size:2em;font-weight:bold;color:{T["accent"]}}}.metric-label{{font-size:.8em;color:{T["label"]};margin-top:4px}}
    .header{{text-align:center;padding:5px}}.header h1{{color:{_tc_header};font-size:1.6em;margin:0}}
    .status{{background:{T["card_bg"]};border-radius:8px;padding:6px 20px;text-align:center;color:{_tc_status};border:1px solid {T["card_border"]};margin-bottom:10px}}
    .stSelectbox label, .stButton button, .stMarkdown, .stSubheader, .stText, .stCaption, .stDataFrame, .stExpander, .stInfo, .stWrite {{color:{_tc} !important}}
    .st-bx {{background:{T["bg"]}}}
    .st-cx {{background:{T["bg"]}}}
    .st-da {{background:{T["bg"]}}}
    </style>""", unsafe_allow_html=True)

    # 加载 KPI + 健康检测 + 关键词总量（全局数据，无时间筛选）
    total_posts, topics, users, akw, d_day, last_post, posts_1m, kw_health, total_raw, df_k = _load_kpi_health()
    
    now = datetime.now().strftime("%H:%M:%S")
    st.markdown(f'<div class="header"><h1>高考话题实时监控中心</h1></div>', unsafe_allow_html=True)

    # ===== 健康检测 =====
    db_ok = last_post is not None
    if db_ok:
        last_dt = pd.to_datetime(last_post)
        seconds_since = (datetime.now() - last_dt).total_seconds()
        flow_ok = seconds_since < 180
        flow_status = "数据流入正常" if flow_ok else "数据流中断!"
        flow_detail = f"最新帖子: {last_dt.strftime('%H:%M:%S')} | 过去1分钟: {posts_1m}条"
    else:
        flow_status = "等待数据中"
        flow_detail = "数据库尚无数据"
    db_status = "数据库正常" if db_ok else "数据库异常"
    st.markdown(f'<div class="status">{db_status} | {flow_status} | {flow_detail} | 总量:{total_raw}条 | 刷新:{now}</div>', unsafe_allow_html=True)

    # ===== 健康详情 =====
    with st.expander("系统健康详情 - 爬虫线程 / 数据校验"):
        hc1, hc2 = st.columns(2)
        with hc1:
            st.markdown("**爬虫线程状态（各关键词最近 5 分钟）**")
            if kw_health:
                dead = [k for k, c in kw_health if c == 0]
                alive_count = len([k for k, c in kw_health if c > 0])
                st.markdown(f"活跃关键词: **{alive_count}**/{len(kw_health)} | " +
                    f"[OK] 有数据 {alive_count} 个" +
                    (f" | [ERR] 异常: {len(dead)}个" if dead else ""))
                for kw, cnt in kw_health:
                    icon = "正常" if cnt > 0 else "异常"
                    st.markdown(f"{icon} **{kw}**: {cnt}条")
            else:
                st.info("暂无数据")
        with hc2:
            st.markdown("**图表数据量校验**")
            st.write(f"· 热度趋势: {total_raw} 条 (video_stats)")
            st.write(f"· 关键词排行: {len(df_k)} 个 (keyword_ranking)")
            st.write(f"· 原始帖子: {total_raw} 条 (raw_posts)")
            st.caption("数据量为 0 表示该组件尚未产出数据，请检查对应服务")

    # KPI
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.markdown(f'<div class="metric-card"><div class="metric-value">{int(total_posts):,}</div><div class="metric-label">帖子总数</div></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card"><div class="metric-value">{topics}</div><div class="metric-label">活跃话题</div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="metric-card"><div class="metric-value">{int(users):,}</div><div class="metric-label">独立用户</div></div>', unsafe_allow_html=True)
    c4.markdown(f'<div class="metric-card"><div class="metric-value">{akw}</div><div class="metric-label">Keywords</div></div>', unsafe_allow_html=True)
    c5.markdown(f'<div class="metric-card"><div class="metric-value">{d_day}</div><div class="metric-label">距高考(天)</div></div>', unsafe_allow_html=True)
    st.markdown("---")

    # ===== 系统配置 =====
    st.subheader("系统配置")
    sc1, sc2, sc3, sc4 = st.columns(4)
    with sc1:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{len(CFG["crawler"]["keywords"])}</div><div class="metric-label">爬虫关键词数</div></div>', unsafe_allow_html=True)
    with sc2:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{CFG["crawler"]["pages"]}</div><div class="metric-label">爬取页数</div></div>', unsafe_allow_html=True)
    with sc3:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{CFG["crawler"]["workers"]}</div><div class="metric-label">爬虫线程数</div></div>', unsafe_allow_html=True)
    with sc4:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{CFG["crawler"]["interval_seconds"]}s</div><div class="metric-label">爬取间隔</div></div>', unsafe_allow_html=True)

    st.markdown("---")

    # ===== 立即刷新按钮 =====
    if st.button("🔄 立即刷新"):
        st.cache_data.clear()
        st.rerun()

    # ===== 第一行: 热度趋势 + 实时速率 =====
    r1c1,r1c2 = st.columns(2)
    with r1c1:
        col1, col2 = st.columns([3, 1])
        with col1: st.subheader("话题热度趋势")
        with col2: _t_trend = st.selectbox("时间", list(TIME_RANGES.keys()), index=3, key="trend_time", label_visibility="collapsed")
        df_v = _query_video_stats(TIME_RANGES[_t_trend])
        if not df_v.empty:
            dv = df_v.groupby(["window_time","keyword"])["video_count"].sum().reset_index().sort_values("window_time")
            top_kws = dv.groupby("keyword")["video_count"].sum().nlargest(8).index.tolist()
            dv = dv[dv["keyword"].isin(top_kws)]
            dv = dv.tail(300)
            fig = px.line(dv, x="window_time", y="video_count", color="keyword", labels={"video_count":"帖子数"})
            fig.update_layout(height=300,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,legend=dict(orientation="h",y=1.15),margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"显示 TOP8 + 其他 · total {df_v['keyword'].nunique()} 个关键词")
        else: 
            st.info("等待数据...")
    
    with r1c2:
        col1, col2 = st.columns([3, 1])
        with col1: st.subheader("帖子实时速率")
        with col2: _t_rate = st.selectbox("时间", list(TIME_RANGES.keys()), index=2, key="rate_time", label_visibility="collapsed")
        df_d = _query_danmaku(TIME_RANGES[_t_rate])
        if not df_d.empty:
            fig = px.bar(df_d.sort_values("window_time").tail(100), x="window_time", y="total", labels={"total":"帖/分钟"})
            fig.update_traces(marker_color="#ff6b35")
            fig.update_layout(height=300,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(df_d)} 个时间窗口")
        else: 
            st.info("等待数据...")
    st.markdown("---")

    # ===== 第二行: 关键词占比 + TOP20 + 情感仪表（无时间选择器） =====
    df_st_pie = _query_sentiment_pie(0)  # 情感仪表+分布使用"所有"
    r2c1,r2c2,r2c3 = st.columns([2,2,1.5])
    with r2c1:
        st.subheader("话题占比树图")
        if not df_k.empty:
            fig = px.treemap(df_k.head(20), path=["keyword"], values="count", color="count", color_continuous_scale="OrRd")
            fig.update_layout(height=350,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"TOP20 keywords · total {df_k['count'].sum():,} 次")
        else: 
            st.info("等待数据...")
    
    with r2c2:
        st.subheader("关键词 TOP20")
        if not df_k.empty:
            dk = df_k.head(20).iloc[::-1]
            fig = px.bar(dk, y="keyword", x="count", orientation="h", color="count", color_continuous_scale="Blues", labels={"count":"出现次数"})
            fig.update_layout(height=350,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,showlegend=False,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(dk)} 个关键词 · 从 {dk['count'].min()} 到 {dk['count'].max()} 次")
        else: 
            st.info("等待数据...")
    
    with r2c3:
        st.subheader("情感仪表")
        if not df_st_pie.empty:
            def _g(s):
                r = df_st_pie[df_st_pie["sentiment"]==s]
                return int(r["cnt"].values[0]) if not r.empty else 0
            pos, neu, neg = _g("positive"), _g("neutral"), _g("negative")
            total = pos + neu + neg
            fig = go.Figure(go.Indicator(mode="gauge+number+delta", value=pos,
                delta={"reference":neg,"decreasing":{"color":"#F44336"}},
                title={"text":"Positive vs Negative"},
                gauge={"axis":{"range":[0,max(total,10)]},"bar":{"color":"#4CAF50"},
                       "steps":[{"range":[0,neu],"color":"#FFC107"}]}))
            fig.update_layout(height=350,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"Cumulative · 正{pos} 中{neu} 负{neg}")
        else: 
            st.info("等待数据...")

    # ===== 第三行: 情感分布（无时间选择器）+ 情感时序河流图 + 小时发帖分布 =====
    r3c1,r3c2,r3c3 = st.columns(3)
    with r3c1:
        st.subheader("情感分布")
        if not df_st_pie.empty:
            def _g(s):
                r = df_st_pie[df_st_pie["sentiment"]==s]
                return int(r["cnt"].values[0]) if not r.empty else 0
            pos, neu, neg = _g("positive"), _g("neutral"), _g("negative")
            fig = go.Figure(go.Pie(labels=["Positive","Neutral","Negative"],
                values=[pos, neu, neg],
                marker_colors=["#4CAF50","#FFC107","#F44336"],hole=0.5,textinfo="label+percent"))
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,showlegend=False,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"累计: 正面{pos} 中{neu} 负{neg}")
        else: 
            st.info("等待数据...")
    
    with r3c2:
        col1, col2 = st.columns([3, 1])
        with col1: st.subheader("情感时序河流图")
        with col2: _t_sent = st.selectbox("时间", list(TIME_RANGES.keys()), index=3, key="sentiment_time", label_visibility="collapsed")
        df_s = _query_sentiment_ts(TIME_RANGES[_t_sent])
        if not df_s.empty:
            ds = df_s.sort_values("window_time")
            fig = go.Figure()
            for cl,nm,co in [("positive","Positive","#4CAF50"),("neutral","Neutral","#FFC107"),("negative","Negative","#F44336")]:
                fig.add_trace(go.Scatter(x=ds["window_time"],y=ds[cl],mode="lines",name=nm,line=dict(color=co,width=2),stackgroup="one"))
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,legend=dict(orientation="h",y=1.15),margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            if not ds.empty:
                st.caption(f"{len(ds)} 个时间窗口 · 覆盖 {ds['window_time'].min().strftime('%H:%M')}~{ds['window_time'].max().strftime('%H:%M')}")
            else:
                st.caption(f"{len(ds)} 个时间窗口")
        else: 
            st.info("等待数据...")
    
    with r3c3:
        _HOUR_RANGES = {k: v for k, v in TIME_RANGES.items() if v >= 60 or v == 0}
        col1, col2 = st.columns([3, 1])
        with col1: st.subheader("小时发帖分布")
        with col2: _t_hour = st.selectbox("时间", list(_HOUR_RANGES.keys()), index=2, key="hourly_time", label_visibility="collapsed")
        df_hourly = _query_hourly_dist(_HOUR_RANGES[_t_hour])
        if not df_hourly.empty:
            fig = px.bar(df_hourly, x="hour", y="video_count", labels={"hour":"小时","video_count":"帖子数"})
            fig.update_traces(marker_color="#2196F3")
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,margin=dict(l=10,r=10,t=10,b=10))
            fig.update_xaxes(dtick=2)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(df_hourly)} 个时段 · 峰值 {int(df_hourly['video_count'].max())} 条")
        else: 
            st.info("等待数据...")
    st.markdown("---")

    # ===== 用户画像分析（所有） =====
    st.subheader("用户画像分析")
    df_gender, df_loc_tot, df_user_tot = _query_profile(0)  # 所有
    r4c1,r4c2,r4c3 = st.columns(3)
    with r4c1:
        st.subheader("性别分布")
        if not df_gender.empty:
            g = df_gender.set_index("gender")["cnt"]
            g.index = g.index.map({"m":"男","f":"女"})
            fig = go.Figure(go.Pie(labels=g.index.tolist(), values=g.values.tolist(),
                marker_colors=["#2196F3","#E91E63"],hole=0.5,textinfo="label+percent"))
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,showlegend=False,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"累计: 男{int(g.get('男',0))} 女{int(g.get('女',0))}")
        else: 
            st.info("等待性别数据...")
    
    with r4c2:
        st.subheader("省份分布地图")
        if not df_loc_tot.empty:
            _PROV = {
                "北京":(116.4,39.9),"上海":(121.5,31.2),"天津":(117.2,39.1),
                "重庆":(106.5,29.6),"广东":(113.3,23.1),"浙江":(120.2,30.3),
                "江苏":(118.8,32.1),"山东":(117.0,36.7),"河南":(113.7,33.9),
                "河北":(114.5,38.0),"湖南":(113.0,28.2),"湖北":(112.0,31.0),
                "四川":(104.0,30.6),"福建":(119.3,26.1),"安徽":(117.3,31.8),
                "辽宁":(123.4,41.8),"陕西":(108.9,34.3),"山西":(112.5,37.9),
                "江西":(115.9,28.7),"广西":(108.4,22.8),"云南":(102.7,25.0),
                "贵州":(106.7,26.6),"甘肃":(103.8,36.0),"吉林":(125.3,43.9),
                "黑龙江":(126.6,45.8),"内蒙古":(111.8,40.8),"新疆":(87.6,47.9),
                "西藏":(91.1,29.6),"青海":(101.8,36.6),"宁夏":(106.3,38.5),
                "海南":(110.3,20.0),"台湾":(121.5,25.0),"香港":(114.2,22.3),
                "澳门":(113.5,22.2),
            }
            loc = df_loc_tot[df_loc_tot["location"].isin(_PROV.keys())]
            if not loc.empty:
                loc = loc.set_index("location")["cnt"]
                df_map = pd.DataFrame([
                    {"province":nm,"count":ct,"lon":_PROV[nm][0],"lat":_PROV[nm][1]}
                    for nm,ct in loc.items()
                ])
                fig = go.Figure()
                fig.add_trace(go.Scattergeo(
                    lon=df_map["lon"], lat=df_map["lat"],
                    text=df_map["province"],
                    hovertext=df_map.apply(lambda r:f"{r['province']}: {r['count']}条",axis=1),
                    mode="markers+text",
                    marker=dict(
                        size=df_map["count"]/df_map["count"].max()*40+10,
                        color=df_map["count"], colorscale="Viridis",
                        showscale=True, colorbar_title="帖子数",
                        line=dict(width=1,color="white"),
                    ),
                    textposition="top center",
                    textfont=dict(size=11,color="#e0e0e0"),
                ))
                fig.update_layout(
                    geo=dict(
                        projection_type="natural earth",
                        showland=True, landcolor="#1a1a2e",
                        showocean=True, oceancolor="#0d1117",
                        showcountries=True, countrycolor="#2a2a4a",
                        showcoastlines=True, coastlinecolor="#2a2a4a",
                        lataxis=dict(range=[15,55]), lonaxis=dict(range=[75,135]),
                    ),
                    height=350, margin=dict(l=5,r=5,t=5,b=5),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    font_color=_fc,
                )
                st.plotly_chart(fig, use_container_width=True)
                total_loc = int(df_map["count"].sum())
                st.caption(f"Cumulative {total_loc} 条 · {len(df_map)} 个省份")
            else:
                loc2 = df_loc_tot.set_index("location")["cnt"]
                if not loc2.empty:
                    fig = px.treemap(path=[loc2.index], values=loc2.values,
                        color=loc2.values, color_continuous_scale="Viridis")
                    fig.update_traces(textinfo="label+value", textfont_size=13)
                    fig.update_layout(height=280, plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)", font_color=_fc,
                        margin=dict(l=5,r=5,t=5,b=5))
                    st.plotly_chart(fig, use_container_width=True)
                else: 
                    st.info("等待省份数据...")
        else: 
            st.info("等待数据...")
    
    with r4c3:
        st.subheader("活跃用户 TOP15（总量）")
        if not df_user_tot.empty:
            du = df_user_tot.set_index("user_name")["cnt"].iloc[::-1]
            fig = px.bar(du, y=du.index, x=du.values, orientation="h", color=du.values,
                        color_continuous_scale="Oranges", labels={"x":"发帖数","y":"用户"})
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color=_fc,showlegend=False,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"Cumulative TOP15 · 最高 {int(du.values[-1])} 条")
        else: 
            st.info("等待用户数据...")
    st.markdown("---")

    # ===== 深度交叉分析（所有） =====
    st.subheader("深度交叉分析")
    df_len_tot, df_heat_tot, df_kw_trend = _query_cross_analysis(0)  # 所有
    da1, da2, da3 = st.columns(3)

    with da1:
        st.subheader("帖子长度分布")
        if not df_len_tot.empty:
            df_len_tot["text_len"] = df_len_tot["text_len"].clip(upper=200)
            fig = px.histogram(df_len_tot, x="text_len", nbins=25, color_discrete_sequence=["#00BCD4"])
            fig.update_layout(height=260, plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)", font_color=_fc,
                margin=dict(l=10,r=10,t=20,b=10),
                xaxis_title="字数", yaxis_title="帖子数")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"基于 {len(df_len_tot)} 条帖子 · 中位数约 {int(df_len_tot['text_len'].median())} 字")
        else: 
            st.info("等待数据...")

    with da2:
        st.subheader("情感 x 关键词热力图")
        if not df_heat_tot.empty:
            df_heat_tot["sentiment_cn"] = df_heat_tot["sentiment"].map(
                {"positive":"正向","neutral":"中性","negative":"负向"})
            fig = px.density_heatmap(df_heat_tot, x="keyword", y="sentiment_cn", z="cnt",
                color_continuous_scale="RdYlGn", text_auto=True)
            fig.update_layout(height=260, plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)", font_color=_fc,
                margin=dict(l=10,r=10,t=20,b=10),
                xaxis_title="", yaxis_title="情感")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"TOP10 keywords x sentiment · cumulative {int(df_heat_tot['cnt'].sum())} 条")
        else: 
            st.info("等待数据...")

    with da3:
        st.subheader("关键词累积增长趋势")
        if not df_kw_trend.empty:
            fig = px.area(df_kw_trend, x="window_time", y="cumulative",
                labels={"cumulative":"累计帖子数","window_time":"时间"},
                color_discrete_sequence=["#00BCD4"])
            fig.update_layout(height=260, plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)", font_color=_fc,
                margin=dict(l=10,r=10,t=20,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"当前累计 {int(df_kw_trend['cumulative'].iloc[-1]):,} 条")
        else: 
            st.info("等待数据...")

    st.markdown("---")

    # ===== 更多分析维度（所有） =====
    st.subheader("更多分析维度")
    df_r = _query_more_analysis(0)  # 所有
    mc1, mc2, mc3 = st.columns(3)

    with mc1:
        st.subheader("地域 x 情感交叉分析")
        if not df_r.empty and len(df_r) >= 10:
            area = df_r[df_r["location"].isin([
                "北京","上海","广东","浙江","江苏","四川","湖北","湖南",
                "山东","河南","福建","安徽","辽宁","陕西","河北","重庆"])]
            if not area.empty:
                area["sentiment_simple"] = area["sentiment"].map(
                    lambda x: {"positive":"正向","neutral":"中性","negative":"负向"}.get(str(x).lower(),"未知"))
                cross = area.groupby(["location","sentiment_simple"]).size().reset_index(name="count")
                fig = px.bar(cross, x="location", y="count", color="sentiment_simple",
                    color_discrete_map={"正向":"#4CAF50","中性":"#FFC107","负向":"#F44336"},
                    barmode="stack", labels={"count":"帖子数","location":"地区"})
                fig.update_layout(height=260, plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)", font_color=_fc,
                    legend=dict(orientation="h",y=1.15), margin=dict(l=10,r=10,t=20,b=10))
                st.plotly_chart(fig, use_container_width=True)
                st.caption(f"{area['location'].nunique()} 个省份 · {len(area)} 条帖子")
            else: 
                st.info("等待地域数据...")
        else: 
            st.info("等待数据...")

    with mc2:
        st.subheader("性别 x 情感交叉分析")
        if not df_r.empty and len(df_r) >= 10:
            gs = df_r[df_r["gender"].isin(["m","f"])].copy()
            if not gs.empty:
                gs["gender_cn"] = gs["gender"].map({"m":"男","f":"女"})
                gs["sentiment_simple"] = gs["sentiment"].map(
                    lambda x: {"positive":"正向","neutral":"中性","negative":"负向"}.get(str(x).lower(),"未知"))
                cross = gs.groupby(["gender_cn","sentiment_simple"]).size().reset_index(name="count")
                fig = px.bar(cross, x="gender_cn", y="count", color="sentiment_simple",
                    color_discrete_map={"正向":"#4CAF50","中性":"#FFC107","负向":"#F44336"},
                    barmode="stack", labels={"count":"帖子数","gender_cn":"性别"})
                fig.update_layout(height=260, plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)", font_color=_fc,
                    legend=dict(orientation="h",y=1.15), margin=dict(l=10,r=10,t=20,b=10))
                st.plotly_chart(fig, use_container_width=True)
                st.caption(f"男{len(gs[gs['gender']=='m'])} 女{len(gs[gs['gender']=='f'])}")
            else: 
                st.info("等待性别数据...")
        else: 
            st.info("等待数据...")

    with mc3:
        st.subheader("用户发帖时段分布")
        if not df_r.empty:
            df_r["hour"] = df_r["window_time"].dt.hour
            hourly_user = df_r.groupby("hour")["user_name"].nunique().reset_index()
            hourly_user.columns = ["hour","users"]
            fig = px.bar(hourly_user, x="hour", y="users",
                labels={"hour":"小时","users":"活跃用户数"},
                color_discrete_sequence=["#AB47BC"])
            fig.update_layout(height=260, plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)", font_color=_fc,
                margin=dict(l=10,r=10,t=20,b=10))
            fig.update_xaxes(dtick=2)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(hourly_user)} 个时段 · 峰值 {int(hourly_user['users'].max())} 人")
        else: 
            st.info("等待数据...")

    st.markdown("---")

    # ===== 词云（所有） =====
    st.subheader("关键词词云")
    wc_bytes = load_wordcloud(0)  # 所有
    if wc_bytes:
        st.image(wc_bytes, use_container_width=True)
        st.caption(f"基于 keyword_ranking TOP100 词频")
    else:
        st.info("等待关键词数据...")

    st.markdown("---")

    # ===== 关键词共现网络 =====
    st.subheader("关键词共现网络")
    _G, _kw_list, _count_dict = load_keyword_network()
    if _G and len(_G.nodes()) > 0:
        _max_n = max((_count_dict.get(n, 1) for n in _G.nodes()), default=1)
        try:
            from pyvis.network import Network
            import tempfile, os
            _bg_hex = T["bg"]  # 主题背景色
            _fc_hex = _fc  # 图表字体色
            net = Network(height="450px", width="100%", bgcolor=_bg_hex, font_color=_fc_hex)
            net.from_nx(_G)
            # 设置节点样式
            for node in net.nodes:
                n = node["id"]
                size = 14 + 28 * (_count_dict.get(n, 1) / _max_n)
                node["size"] = size
                node["color"] = "#ff6b35"
                node["font"] = {"size": 13, "color": _fc_hex}
                node["title"] = f"{n}: {_count_dict.get(n, 0)}次"
                node["borderWidth"] = 2
                node["borderWidthSelected"] = 4
            # 设置边样式
            for edge in net.edges:
                edge["color"] = "#4a6a9a"
                edge["width"] = 2
                edge["smooth"] = {"enabled": True, "type": "continuous"}
                edge["opacity"] = 0.5
            # 物理引擎 + 交互配置
            net.set_options("""
            {
              "physics": {
                "barnesHut": {
                  "gravitationalConstant": -2500,
                  "centralGravity": 0.2,
                  "springLength": 180,
                  "springConstant": 0.03,
                  "damping": 0.08
                },
                "minVelocity": 0.5,
                "solver": "barnesHut",
                "stabilization": {"iterations": 300, "updateInterval": 10}
              },
              "edges": {"smooth": {"enabled": true, "type": "continuous"}},
              "interaction": {
                "dragNodes": true,
                "dragView": true,
                "zoomView": true,
                "hover": true,
                "tooltipDelay": 200,
                "multiselect": false
              }
            }
            """)
            _tmp = os.path.join(tempfile.gettempdir(), "kw_network.html")
            net.save_graph(_tmp)
            with open(_tmp, "r", encoding="utf-8") as f:
                html = f.read()
            # 把 HTML body 背景也改成主题色
            html = html.replace("<body>", f'<body style="background:{_bg_hex};">')
            st.components.v1.html(html, height=460, scrolling=False)
            st.caption(f"📌 拖拽节点自动弹开 · 节点越大热度越高 · 连线=关键词同现 · 数据来源最近500条帖子 · {len(_G.nodes())}词 {len(_G.edges())}条关联")
        except Exception as e:
            st.error(f"网络图加载失败: {e}")
    else:
        st.info("等待足够数据构建网络（至少需要2个关键词之间有共现关系）...")

    st.markdown("---")

    # ===== 话题聚类（所有） =====
    st.subheader("话题聚类分析（TF-IDF + KMeans 无监督聚类）")
    topic_counts, topic_samples = load_topics(0)  # 所有
    tc1, tc2 = st.columns([1.5, 2.5])
    with tc1:
        if not topic_counts.empty:
            fig = go.Figure(go.Pie(
                labels=topic_counts["topic"],
                values=topic_counts["count"],
                hole=0.45,
                textinfo="label+percent",
                textfont=dict(size=11),
                marker=dict(colors=px.colors.qualitative.Plotly),
            ))
            fig.update_layout(
                height=380,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color=_fc,
                showlegend=False,
                margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(topic_counts)} 个话题簇 · 基于 raw_posts 最新500条聚类")
        else:
            st.info("等待足够数据（至少需要10条有效帖子）")
    
    with tc2:
        if not topic_samples.empty:
            for _, row in topic_samples.iterrows():
                topic = row["topic"]
                samples = row["text_preview"]
                count_row = topic_counts[topic_counts["topic"] == topic]
                cnt = count_row["count"].values[0] if not count_row.empty else 0
                with st.expander(f"{topic} —— {cnt} 条"):
                    for s in samples[:5]:
                        st.markdown(f"> {s}")
        else:
            st.info("等待足够数据...")

    # ===== 关键词排行明细 + 最新帖子动态 =====
    r5c1,r5c2 = st.columns(2)
    with r5c1:
        st.subheader("关键词排行明细（总量）")
        if not df_k.empty:
            st.dataframe(df_k.head(15), use_container_width=True, hide_index=True)
    
    with r5c2:
        st.subheader("最新帖子动态（所有）")
        if not df_r.empty:
            latest = df_r.head(15)[["window_time","keyword","user_name","gender","location","sentiment","text_preview"]].copy()
            latest["window_time"] = latest["window_time"].astype(str).str[:19]
            sent_map = {"positive":"正向","neutral":"中性","negative":"负向"}
            latest["sentiment"] = latest["sentiment"].map(lambda x: sent_map.get(str(x).lower(),"未知"))
            latest["gender"] = latest["gender"].map({"m":"男","f":"女","":"未知"})
            # 保留帖子内容中的 emoji
            latest.columns = ["时间","关键词","用户","性别","地区","情感","内容预览"]
            st.dataframe(latest, use_container_width=True, hide_index=True)
        else: 
            st.info("等待数据...")

    # 情感明细
    st.subheader("情感统计明细（所有）")
    if not df_s.empty:
        ds2 = df_s.head(10)[["window_time","positive","neutral","negative"]].copy()
        ds2["window_time"] = ds2["window_time"].astype(str)
        ds2.columns = ["时间","正面","中性","负面"]
        st.dataframe(ds2, use_container_width=True, hide_index=True)

    # ===== 访客统计 =====
    _total_visitors, _online_users = _init_visitor()
    st.markdown("---")
    _online_icon = "🟢" if _online_users > 0 else "⚪"
    st.markdown(f'<div style="display:flex;align-items:center;gap:20px;color:{_fc};font-size:0.85em">'
                f'<span>👁️ 访问量 <strong>{_total_visitors}</strong></span>'
                f'<span>{_online_icon} 在线 <strong>{_online_users}</strong></span>'
                f'<span style="margin-left:auto">每15秒自动刷新 | 最后更新: {now} | 高考话题实时监控中心 v5</span>'
                f'</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()
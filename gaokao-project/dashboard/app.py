# -*- coding: utf-8 -*-
"""
Gaokao Topic Real-time Monitor v5
聚焦: 帖子正文分析 + 发帖量 + 关键词 + 情感 + 用户分析
"""
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys, os, json, re

# 加载配置
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as _f:
    CFG = json.load(_f)
MYSQL = CFG["mysql"]

st.set_page_config(page_title="高考监控中心", layout="wide")

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

@st.cache_data(ttl=15)
def load(time_hours=120, profile_hours=1440):
    """加载数据，支持时间范围参数"""
    conn = mysql.connector.connect(**MYSQL)
    time_cond = get_time_condition(time_hours)
    profile_cond = get_time_condition(profile_hours)
    
    # 帖子统计(按关键词)
    try:
        df_v = pd.read_sql(f"SELECT window_time,keyword,video_count FROM video_stats WHERE {time_cond} ORDER BY window_time", conn)
        if not df_v.empty: 
            df_v["window_time"] = pd.to_datetime(df_v["window_time"])
    except: 
        df_v = pd.DataFrame()
    
    # 发帖速率
    try:
        df_d = pd.read_sql(f"SELECT window_time,SUM(count) as total FROM danmaku_per_minute WHERE {time_cond} GROUP BY window_time ORDER BY window_time", conn)
        if not df_d.empty: 
            df_d["window_time"] = pd.to_datetime(df_d["window_time"])
    except: 
        df_d = pd.DataFrame()
    
    # 关键词
    try:
        df_k = pd.read_sql("SELECT keyword,count FROM keyword_ranking ORDER BY count DESC LIMIT 30", conn)
    except: 
        df_k = pd.DataFrame()
    
    # 情感
    try:
        df_s = pd.read_sql(f"SELECT window_time,positive,neutral,negative FROM sentiment_per_minute WHERE {time_cond} ORDER BY window_time DESC LIMIT 120", conn)
        if not df_s.empty: 
            df_s["window_time"] = pd.to_datetime(df_s["window_time"])
    except: 
        df_s = pd.DataFrame()
    
    # 活跃用户
    try:
        df_u = pd.read_sql("SELECT keyword as user_name,rate as post_count FROM engagement_rate WHERE window_time='2026-01-01 00:00:00' ORDER BY rate DESC LIMIT 20", conn)
    except: 
        df_u = pd.DataFrame()
    
    # 原始帖子
    try:
        df_r = pd.read_sql(f"SELECT keyword,user_name,gender,location,sentiment,text_preview,window_time FROM raw_posts WHERE {time_cond} ORDER BY id DESC LIMIT 100", conn)
        if not df_r.empty: 
            df_r["window_time"] = pd.to_datetime(df_r["window_time"])
    except: 
        df_r = pd.DataFrame()
    
    # KPI
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(video_count),0) FROM video_stats"); total_posts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT keyword) FROM video_stats"); topics = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT user_name) FROM raw_posts"); users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM keyword_ranking WHERE count>0"); akw = cur.fetchone()[0]
    
    # ===== 健康检测 =====
    try:
        cur.execute("SELECT MAX(window_time) FROM raw_posts")
        last_post = cur.fetchone()[0]
    except:
        last_post = None
    try:
        cur.execute("SELECT COUNT(*) FROM raw_posts WHERE window_time>=NOW()-INTERVAL 1 MINUTE")
        posts_1m = cur.fetchone()[0]
    except:
        posts_1m = 0
    
    # 各关键词最近5分钟帖子数
    try:
        cur.execute("""
            SELECT keyword, COUNT(*) as cnt
            FROM raw_posts
            WHERE window_time >= NOW()-INTERVAL 5 MINUTE
            GROUP BY keyword ORDER BY cnt DESC
        """)
        kw_health = cur.fetchall()
    except:
        kw_health = []
    
    # 各图表数据量
    try:
        cur.execute(f"SELECT COUNT(*) FROM video_stats WHERE {time_cond}")
        chart_v_rows = cur.fetchone()[0]
    except:
        chart_v_rows = 0
    try:
        cur.execute("SELECT COUNT(*) FROM keyword_ranking WHERE count>0")
        chart_k_rows = cur.fetchone()[0]
    except:
        chart_k_rows = 0
    try:
        cur.execute("SELECT COUNT(*) FROM sentiment_per_minute")
        chart_s_rows = cur.fetchone()[0]
    except:
        chart_s_rows = 0
    try:
        cur.execute("SELECT COUNT(*) FROM raw_posts")
        total_raw = cur.fetchone()[0]
    except:
        total_raw = 0
    
    # ===== 总量情感（从 raw_posts 累计） =====
    try:
        df_st = pd.read_sql(
            f"SELECT sentiment, COUNT(*) as cnt FROM raw_posts "
            f"WHERE sentiment IN ('positive','neutral','negative') AND {time_cond} "
            "GROUP BY sentiment", conn)
    except:
        df_st = pd.DataFrame()
    
    # ===== 用户画像 - 总量 =====
    try:
        df_gender = pd.read_sql(
            f"SELECT gender, COUNT(*) as cnt FROM raw_posts "
            f"WHERE gender IN ('m','f') AND {profile_cond} GROUP BY gender", conn)
    except:
        df_gender = pd.DataFrame()
    try:
        df_loc_tot = pd.read_sql(
            f"SELECT location, COUNT(*) as cnt FROM raw_posts "
            f"WHERE location!='' AND location NOT IN ('其他','其它') AND {profile_cond} "
            "GROUP BY location ORDER BY cnt DESC LIMIT 20", conn)
    except:
        df_loc_tot = pd.DataFrame()
    try:
        df_user_tot = pd.read_sql(
            f"SELECT user_name, COUNT(*) as cnt FROM raw_posts "
            f"WHERE user_name!='' AND user_name!='?' AND {profile_cond} "
            "GROUP BY user_name ORDER BY cnt DESC LIMIT 15", conn)
    except:
        df_user_tot = pd.DataFrame()
    
    # ===== 关键词增长趋势 =====
    try:
        df_kw_trend = pd.read_sql(
            f"SELECT window_time, SUM(video_count) as cumulative "
            f"FROM video_stats WHERE {time_cond} GROUP BY window_time ORDER BY window_time",
            conn)
        if not df_kw_trend.empty:
            df_kw_trend["window_time"] = pd.to_datetime(df_kw_trend["window_time"])
            df_kw_trend["cumulative"] = df_kw_trend["cumulative"].cumsum()
    except:
        df_kw_trend = pd.DataFrame()
    
    # ===== 帖子长度分布 =====
    try:
        df_len_tot = pd.read_sql(
            f"SELECT LENGTH(text_preview) as text_len FROM raw_posts "
            f"WHERE LENGTH(text_preview) BETWEEN 1 AND 300 AND {time_cond} "
            "ORDER BY id DESC LIMIT 2000", conn)
    except:
        df_len_tot = pd.DataFrame()
    
    # ===== 情感×关键词热力 =====
    try:
        df_heat_tot = pd.read_sql(
            f"SELECT keyword, sentiment, COUNT(*) as cnt FROM raw_posts "
            f"WHERE sentiment IN ('positive','neutral','negative') AND {time_cond} "
            "AND keyword IN (SELECT keyword FROM keyword_ranking ORDER BY count DESC LIMIT 10) "
            "GROUP BY keyword, sentiment", conn)
    except:
        df_heat_tot = pd.DataFrame()
    
    conn.close()
    d_day = (datetime(2026,6,7)-datetime.now()).days
    return (df_v,df_d,df_k,df_s,df_u,df_r,df_st,df_kw_trend,
            df_gender,df_loc_tot,df_user_tot,df_len_tot,df_heat_tot,
            total_posts,topics,users,akw,d_day,
            last_post,posts_1m,kw_health,
            chart_v_rows,chart_k_rows,chart_s_rows,total_raw)

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
            "ORDER BY id DESC LIMIT 500",
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

def main():
    st.markdown("""<style>
    .stApp{background:#0d1117}.metric-card{background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:12px;padding:16px;text-align:center;border:1px solid #2a2a4a}
    .metric-value{font-size:2em;font-weight:bold;color:#ff6b35}.metric-label{font-size:.8em;color:#8b8b9e;margin-top:4px}
    .header{text-align:center;padding:5px}.header h1{color:#e0e0e0;font-size:1.6em;margin:0}
    .status{background:#1a1a2e;border-radius:8px;padding:6px 20px;text-align:center;color:#aaa;border:1px solid #2a2a4a;margin-bottom:10px}
    </style>""", unsafe_allow_html=True)

    # ===== 独立时间范围选择器 =====
    st.sidebar.subheader("时间范围配置")
    
    time_range_trend = st.sidebar.selectbox(
        "热度趋势", 
        options=list(TIME_RANGES.keys()), 
        index=3,  # 默认2小时
        key="trend_time"
    )
    
    time_range_rate = st.sidebar.selectbox(
        "实时速率", 
        options=list(TIME_RANGES.keys()), 
        index=2,  # 默认1小时
        key="rate_time"
    )
    
    time_range_cross = st.sidebar.selectbox(
        "交叉分析", 
        options=list(TIME_RANGES.keys()), 
        index=3,  # 默认2小时
        key="cross_time"
    )
    
    time_range_wordcloud = st.sidebar.selectbox(
        "词云", 
        options=list(TIME_RANGES.keys()), 
        index=5,  # 默认12小时
        key="wordcloud_time"
    )
    
    # 更多分析维度的时间范围选择器
    time_range_profile = st.sidebar.selectbox(
        "用户画像", 
        options=list(TIME_RANGES.keys()), 
        index=6,  # 默认24小时
        key="profile_time"
    )
    
    time_range_topic = st.sidebar.selectbox(
        "话题聚类", 
        options=list(TIME_RANGES.keys()), 
        index=5,  # 默认12小时
        key="topic_time"
    )
    
    time_range_sentiment = st.sidebar.selectbox(
        "情感分析", 
        options=list(TIME_RANGES.keys()), 
        index=3,  # 默认2小时
        key="sentiment_time"
    )
    
    # 加载数据（使用交叉分析的时间范围作为主时间范围）
    main_time = TIME_RANGES[time_range_cross]
    profile_time = TIME_RANGES[time_range_profile]
    topic_time = TIME_RANGES[time_range_topic]
    sentiment_time = TIME_RANGES[time_range_sentiment]
    
    (df_v,df_d,df_k,df_s,df_u,df_r,df_st,df_kw_trend,
     df_gender,df_loc_tot,df_user_tot,df_len_tot,df_heat_tot,
     total_posts,topics,users,akw,d_day,
     last_post,posts_1m,kw_health,
     chart_v_rows,chart_k_rows,chart_s_rows,total_raw) = load(main_time, profile_time)
    
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
            checks = [
                ("热度趋势", chart_v_rows, 1, "video_stats"),
                ("关键词排行", chart_k_rows, 1, "keyword_ranking"),
                ("情感总量", len(df_st), 1, "raw_posts(sentiment)"),
                ("原始帖子", total_raw, 1, "raw_posts"),
            ]
            for name, count, _, table in checks:
                icon = "正常" if count > 0 else "无数据"
                st.markdown(f"{icon} **{name}**: {count} 行 (`{table}`)")
            st.caption("数据量为 0 表示该组件尚未产出数据，请检查对应服务")

    # KPI
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.markdown(f'<div class="metric-card"><div class="metric-value">{int(total_posts):,}</div><div class="metric-label">帖子总数</div></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card"><div class="metric-value">{topics}</div><div class="metric-label">活跃话题</div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="metric-card"><div class="metric-value">{int(users):,}</div><div class="metric-label">独立用户</div></div>', unsafe_allow_html=True)
    c4.markdown(f'<div class="metric-card"><div class="metric-value">{akw}</div><div class="metric-label">Keywords</div></div>', unsafe_allow_html=True)
    c5.markdown(f'<div class="metric-card"><div class="metric-value">{d_day}</div><div class="metric-label">距高考(天)</div></div>', unsafe_allow_html=True)
    st.markdown("---")

    # 第一行: 热度趋势 + 实时速率
    r1c1,r1c2 = st.columns(2)
    with r1c1:
        st.subheader(f"话题热度趋势（{time_range_trend}）")
        if not df_v.empty:
            dv = df_v.groupby(["window_time","keyword"])["video_count"].sum().reset_index().sort_values("window_time")
            top_kws = dv.groupby("keyword")["video_count"].sum().nlargest(8).index.tolist()
            dv = dv[dv["keyword"].isin(top_kws)]
            dv = dv.tail(300)
            fig = px.line(dv, x="window_time", y="video_count", color="keyword", labels={"video_count":"帖子数"})
            fig.update_layout(height=300,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",legend=dict(orientation="h",y=1.15),margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"显示 TOP8 + 其他 · total {df_v['keyword'].nunique()} 个关键词")
        else: 
            st.info("等待数据...")
    
    with r1c2:
        st.subheader(f"帖子实时速率（{time_range_rate}）")
        if not df_d.empty:
            fig = px.bar(df_d.sort_values("window_time").tail(100), x="window_time", y="total", labels={"total":"帖/分钟"})
            fig.update_traces(marker_color="#ff6b35")
            fig.update_layout(height=300,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(df_d)} 个时间窗口")
        else: 
            st.info("等待数据...")
    st.markdown("---")

    # 第二行: 关键词占比 + TOP20 + 情感饼图
    r2c1,r2c2,r2c3 = st.columns([2,2,1.5])
    with r2c1:
        st.subheader("话题占比树图")
        if not df_k.empty:
            fig = px.treemap(df_k.head(20), path=["keyword"], values="count", color="count", color_continuous_scale="OrRd")
            fig.update_layout(height=350,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"TOP20 keywords · total {df_k['count'].sum():,} 次")
        else: 
            st.info("等待数据...")
    
    with r2c2:
        st.subheader("关键词 TOP20")
        if not df_k.empty:
            dk = df_k.head(20).iloc[::-1]
            fig = px.bar(dk, y="keyword", x="count", orientation="h", color="count", color_continuous_scale="Blues", labels={"count":"出现次数"})
            fig.update_layout(height=350,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",showlegend=False,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(dk)} 个关键词 · 从 {dk['count'].min()} 到 {dk['count'].max()} 次")
        else: 
            st.info("等待数据...")
    
    with r2c3:
        st.subheader(f"情感仪表（{time_range_sentiment}）")
        if not df_st.empty:
            def _g(s):
                r = df_st[df_st["sentiment"]==s]
                return int(r["cnt"].values[0]) if not r.empty else 0
            pos, neu, neg = _g("positive"), _g("neutral"), _g("negative")
            total = pos + neu + neg
            fig = go.Figure(go.Indicator(mode="gauge+number+delta", value=pos,
                delta={"reference":neg,"decreasing":{"color":"#F44336"}},
                title={"text":"Positive vs Negative"},
                gauge={"axis":{"range":[0,max(total,10)]},"bar":{"color":"#4CAF50"},
                       "steps":[{"range":[0,neu],"color":"#FFC107"}]}))
            fig.update_layout(height=350,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"Cumulative · 正{pos} 中{neu} 负{neg}")
        else: 
            st.info("等待数据...")

    # 第三行: 情感时序 + 小时分布 + 帖子长度
    r3c1,r3c2,r3c3 = st.columns(3)
    with r3c1:
        st.subheader(f"情感分布（{time_range_sentiment}）")
        if not df_st.empty:
            def _g(s):
                r = df_st[df_st["sentiment"]==s]
                return int(r["cnt"].values[0]) if not r.empty else 0
            pos, neu, neg = _g("positive"), _g("neutral"), _g("negative")
            fig = go.Figure(go.Pie(labels=["Positive","Neutral","Negative"],
                values=[pos, neu, neg],
                marker_colors=["#4CAF50","#FFC107","#F44336"],hole=0.5,textinfo="label+percent"))
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",showlegend=False,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"累计: 正面{pos} 中{neu} 负{neg}")
        else: 
            st.info("等待数据...")
    
    with r3c2:
        st.subheader("情感时序河流图")
        if not df_s.empty:
            ds = df_s.sort_values("window_time")
            fig = go.Figure()
            for cl,nm,co in [("positive","Positive","#4CAF50"),("neutral","Neutral","#FFC107"),("negative","Negative","#F44336")]:
                fig.add_trace(go.Scatter(x=ds["window_time"],y=ds[cl],mode="lines",name=nm,line=dict(color=co,width=2),stackgroup="one"))
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",legend=dict(orientation="h",y=1.15),margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            if not ds.empty:
                st.caption(f"{len(ds)} 个时间窗口 · 覆盖 {ds['window_time'].min().strftime('%H:%M')}~{ds['window_time'].max().strftime('%H:%M')}")
            else:
                st.caption(f"{len(ds)} 个时间窗口")
        else: 
            st.info("等待数据...")
    
    with r3c3:
        st.subheader("小时发帖分布")
        if not df_v.empty:
            df_v["hour"] = df_v["window_time"].dt.hour
            hourly = df_v.groupby("hour")["video_count"].sum().reset_index()
            fig = px.bar(hourly, x="hour", y="video_count", labels={"hour":"小时","video_count":"帖子数"})
            fig.update_traces(marker_color="#2196F3")
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",margin=dict(l=10,r=10,t=10,b=10))
            fig.update_xaxes(dtick=2)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(hourly)} 个时段 · 峰值 {int(hourly['video_count'].max())} 条")
        else: 
            st.info("等待数据...")
    st.markdown("---")

    # ===== 系统状态 =====
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

    # 第四行: 用户画像
    st.subheader(f"用户画像分析（{time_range_profile}）")
    r4c1,r4c2,r4c3 = st.columns(3)
    with r4c1:
        st.subheader("性别分布")
        if not df_gender.empty:
            g = df_gender.set_index("gender")["cnt"]
            g.index = g.index.map({"m":"男","f":"女"})
            fig = go.Figure(go.Pie(labels=g.index.tolist(), values=g.values.tolist(),
                marker_colors=["#2196F3","#E91E63"],hole=0.5,textinfo="label+percent"))
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",showlegend=False,margin=dict(l=10,r=10,t=10,b=10))
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
                    font_color="#ccc",
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
                        paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
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
            fig.update_layout(height=280,plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",font_color="#ccc",showlegend=False,margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"Cumulative TOP15 · 最高 {int(du.values[-1])} 条")
        else: 
            st.info("等待用户数据...")
    st.markdown("---")

    # ===== 深度交叉分析 =====
    st.subheader("深度交叉分析（{}）".format(time_range_cross))
    da1, da2, da3 = st.columns(3)

    with da1:
        st.subheader("帖子长度分布")
        if not df_len_tot.empty:
            df_len_tot["text_len"] = df_len_tot["text_len"].clip(upper=200)
            fig = px.histogram(df_len_tot, x="text_len", nbins=25, color_discrete_sequence=["#00BCD4"])
            fig.update_layout(height=260, plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
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
                paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
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
                paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
                margin=dict(l=10,r=10,t=20,b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"当前累计 {int(df_kw_trend['cumulative'].iloc[-1]):,} 条")
        else: 
            st.info("等待数据...")

    st.markdown("---")

    # ===== 更多分析维度 =====
    st.subheader("更多分析维度")
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
                    paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
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
                    paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
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
                paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
                margin=dict(l=10,r=10,t=20,b=10))
            fig.update_xaxes(dtick=2)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(hourly_user)} 个时段 · 峰值 {int(hourly_user['users'].max())} 人")
        else: 
            st.info("等待数据...")

    st.markdown("---")

    # ===== 词云 =====
    st.subheader(f"关键词词云（{time_range_wordcloud}）")
    wc_bytes = load_wordcloud(TIME_RANGES[time_range_wordcloud])
    if wc_bytes:
        st.image(wc_bytes, use_container_width=True)
        st.caption(f"基于 keyword_ranking TOP100 词频")
    else:
        st.info("等待关键词数据...")

    st.markdown("---")

    # ===== 话题聚类 =====
    st.subheader(f"话题聚类分析（TF-IDF + KMeans 无监督聚类）（{time_range_topic}）")
    topic_counts, topic_samples = load_topics(topic_time)
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
                font_color="#ccc",
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
        st.subheader("关键词排行明细")
        if not df_k.empty:
            st.dataframe(df_k.head(15), use_container_width=True, hide_index=True)
    
    with r5c2:
        st.subheader("最新帖子动态")
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
    st.subheader("情感统计明细")
    if not df_s.empty:
        ds2 = df_s.head(10)[["window_time","positive","neutral","negative"]].copy()
        ds2["window_time"] = ds2["window_time"].astype(str)
        ds2.columns = ["时间","正面","中性","负面"]
        st.dataframe(ds2, use_container_width=True, hide_index=True)

    st.caption(f"每15秒自动刷新 | 最后更新: {now}")

if __name__ == "__main__":
    main()
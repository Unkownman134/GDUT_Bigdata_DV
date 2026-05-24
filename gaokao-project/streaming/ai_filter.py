# -*- coding: utf-8 -*-
"""
帖子质量评分 + 话题聚类 + 高考相关性过滤
基于 TF-IDF + KMeans + 规则启发式方法，不依赖预训练模型
"""
import jieba, re, os

# jieba 缓存目录设到项目目录，避免占用 C 盘
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJ_DIR = os.path.dirname(_THIS_DIR)
os.environ["JIEBA_CACHE_DIR"] = os.path.join(_PROJ_DIR, ".cache")

# 噪音模式: 低信息量内容
NOISE_PATTERNS = [
    "666", "哈哈", "第一", "打卡", "来了", "前排", "火钳刘明",
    "1", "2", "3", "加油", "冲冲冲", "NB", "nb", "牛逼",
    "啊啊啊", "hhhh", "www", "??", "。。", "！！",
]

# ===== 高考相关性过滤 =====
# 排除词: 包含以下任一词汇的帖子直接丢弃（营销/广告/无关话题）
EXCLUDE_TERMS = [
    "亲子鉴定", "DNA鉴定", "征婚", "交友平台", "上门按摩",
    "代孕", "试管婴儿", "不孕不育",
    "算卦", "算命", "风水大师", "星座运势",
    "抽奖活动", "领取红包", "扫码关注", "点击下单", "限时抢购", "拼单",
    "同城约", "上门服务", "同城交友", "找对象", "相亲", "附近的人",
    "减肥瘦身", "美白护肤", "祛痘", "美容院", "整容", "隆鼻", "双眼皮", "植发",
    "兼职日结", "月入过万", "代理招商", "微商加盟", "创业项目", "一件代发",
    "贷款咨询", "信用卡套现", "炒股群", "理财课程", "稳赚不赔",
    "彩票中奖", "棋牌娱乐", "赌博平台", "时时彩", "六合彩",
    "追星打榜", "应援站子", "爱豆", "粉丝群",
    "耽美小说", "同人文", "bl漫画",
    "货源批发", "招代理", "薅羊毛", "领券",
    "巢湖", "夏日童趣", "知识挑战赛",
    "恐怖故事", "灵异事件",
]

# 高考核心词: 命中越多越相关
GAOKAO_CORE = {
    "高考", "高三", "分数线", "志愿", "录取", "复读", "查分", "估分",
    "落榜", "上岸", "一本", "二本", "专科", "985", "211", "双一流",
    "强基计划", "提前批", "调剂", "退档", "滑档", "征集志愿", "平行志愿",
    "模考", "一模", "二模", "三模", "月考", "期中", "期末",
    "真题", "答案卷", "作文素材", "听力", "选专业", "押题", "错题",
    "高考倒计时", "录取结果", "大学排名", "专业选择", "提分", "补课",
    "考生", "高校", "招生", "报名", "备考", "分科", "学霸", "学渣",
    "补习", "辅导", "艺考", "体育生", "单招", "保送", "保研",
    "出国留学", "考公", "考编", "研究生", "专升本", "高职",
}

# 高考相关话题标签匹配
_GAOKAO_TOPIC_RX = re.compile(r"#.*?(高考|高三|志愿|录取|大学|考试|加油|必胜|倒计时).*?#")


def gaokao_relevance(text: str) -> float:
    """高考相关性评分: 0.0（完全无关）到 1.0（高度相关）"""
    text = str(text).strip()
    if not text or len(text) < 4:
        return 0.0

    # 排除词扫描
    for t in EXCLUDE_TERMS:
        if t in text:
            return 0.0

    score = 0.0

    # 核心词命中（每个 +0.10，上限 1.0）
    for kw in GAOKAO_CORE:
        if kw in text:
            score += 0.10
            if score >= 1.0:
                return 1.0

    # 话题标签加分
    if _GAOKAO_TOPIC_RX.search(text):
        score += 0.20

    return min(1.0, score)

# 全局状态: 训练好的向量器和聚类模型
_vectorizer = None
_kmeans = None
_TOPIC_WORDS = {}


def quality_score(text):
    """帖子质量评分: 0-100，高分=有意义内容，低分=噪音"""
    text = str(text).strip()
    if not text:
        return 0

    score = 0

    # 1. 长度评分 (0-30)
    length = len(text)
    if length >= 10:
        score += 30
    elif length >= 6:
        score += 20
    elif length >= 3:
        score += 10
    else:
        score += 0

    # 2. 噪音检测 (0-30)
    is_noise = False
    for pattern in NOISE_PATTERNS:
        if pattern in text:
            is_noise = True
            break
    if not is_noise:
        score += 30
    elif len(text) > 6:  # 文本足够长时，即使包含噪音词也可能有意义
        score += 15

    # 3. 词汇丰富度 (0-20)
    words = jieba.lcut(text)
    if len(words) > 0:
        unique_ratio = len(set(words)) / len(words)
        score += int(unique_ratio * 20)

    # 4. 中文字符占比 (0-10)
    cn_count = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    score += min(10, cn_count)

    # 5. 纯数字/符号惩罚
    if text.isdigit():
        score = max(0, score - 40)

    return min(100, score)


def is_quality_danmaku(text, threshold=25):
    """质量过滤: 质量分达标且与高考相关"""
    if quality_score(text) < threshold:
        return False
    # 高考相关性过滤
    if gaokao_relevance(text) < 0.10:
        return False
    return True


def train_topic_model(texts, n_topics=8):
    """训练 TF-IDF + KMeans 话题聚类模型"""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.cluster import KMeans
    import numpy as np
    global _vectorizer, _kmeans, _TOPIC_WORDS

    # 清洗并分词
    cleaned = []
    for t in texts:
        t = str(t).strip()
        if len(t) < 3:
            continue
        words = jieba.lcut(t)
        cleaned.append(" ".join([w for w in words if len(w) >= 2]))

    if len(cleaned) < n_topics * 3:
        return {}

    _vectorizer = TfidfVectorizer(max_features=500)
    try:
        X = _vectorizer.fit_transform(cleaned)
    except:
        return {}

    n_clusters = min(n_topics, len(cleaned) // 3)
    if n_clusters < 2:
        return {}

    _kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=5)
    _kmeans.fit(X)

    # 每个聚类的主题词
    feature_names = _vectorizer.get_feature_names_out()
    for i in range(n_clusters):
        center = _kmeans.cluster_centers_[i]
        top_idx = center.argsort()[-5:][::-1]
        _TOPIC_WORDS[i] = [feature_names[j] for j in top_idx]

    return _TOPIC_WORDS


def predict_topic(text):
    """预测帖子所属话题类别"""
    if _vectorizer is None or _kmeans is None:
        return "未分类"

    words = jieba.lcut(str(text))
    cleaned = " ".join([w for w in words if len(w) >= 2])

    try:
        X = _vectorizer.transform([cleaned])
        cluster = _kmeans.predict(X)[0]
        words_list = _TOPIC_WORDS.get(cluster, ["未知"])
        return "+".join(words_list[:3])
    except:
        return "未分类"

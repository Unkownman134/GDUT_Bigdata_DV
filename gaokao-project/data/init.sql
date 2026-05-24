-- ========================================
-- 高考话题实时分析项目 - 数据库初始化
-- 数据源: 微博 (Weibo) | 架构: Kafka→Spark→MySQL→Streamlit
-- ========================================
CREATE DATABASE IF NOT EXISTS gaokao CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE gaokao;

-- 清空重建
DROP TABLE IF EXISTS raw_posts;
DROP TABLE IF EXISTS danmaku_per_minute;
DROP TABLE IF EXISTS keyword_ranking;
DROP TABLE IF EXISTS sentiment_per_minute;
DROP TABLE IF EXISTS university_ranking;
DROP TABLE IF EXISTS video_stats;
DROP TABLE IF EXISTS engagement_rate;

-- 1. 帖子统计（按时间窗口+关键词）
--    total_play   → reposts (转发数)
--    total_like   → likes (点赞数)
--    total_reply  → comments_count (评论数)
--    total_danmaku → 帖子数
CREATE TABLE video_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_time DATETIME NOT NULL,
    keyword VARCHAR(50) NOT NULL DEFAULT '',
    video_count INT DEFAULT 0,
    total_play BIGINT DEFAULT 0,
    total_like BIGINT DEFAULT 0,
    total_reply BIGINT DEFAULT 0,
    total_danmaku INT DEFAULT 0,
    UNIQUE KEY uk_ts_kw (window_time, keyword)
);

-- 2. 帖子每分钟计数
CREATE TABLE danmaku_per_minute (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_time DATETIME NOT NULL,
    video_title VARCHAR(255) DEFAULT '',
    count INT DEFAULT 0,
    UNIQUE KEY uk_time (window_time)
);

-- 3. 关键词排行
CREATE TABLE keyword_ranking (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    keyword VARCHAR(50) NOT NULL,
    count INT DEFAULT 0,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_keyword (keyword)
);

-- 4. 情感分布
CREATE TABLE sentiment_per_minute (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_time DATETIME NOT NULL,
    positive INT DEFAULT 0,
    neutral INT DEFAULT 0,
    negative INT DEFAULT 0,
    UNIQUE KEY uk_time (window_time)
);

-- 5. 互动率 (点赞+评论+转发)/帖子数 *100
CREATE TABLE engagement_rate (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_time DATETIME NOT NULL,
    keyword VARCHAR(50) NOT NULL DEFAULT '',
    rate FLOAT DEFAULT 0,
    UNIQUE KEY uk_ts_kw (window_time, keyword)
);

-- 6. 大学/专业提及（备用）
CREATE TABLE university_ranking (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(20) DEFAULT 'university',
    count INT DEFAULT 0,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_name (name)
);

-- 7. 原始帖子明细（用于深度分析）
CREATE TABLE raw_posts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    keyword VARCHAR(50) NOT NULL DEFAULT '',
    user_name VARCHAR(100) DEFAULT '',
    gender VARCHAR(10) DEFAULT '',
    location VARCHAR(50) DEFAULT '',

    sentiment VARCHAR(20) DEFAULT '',
    text_preview VARCHAR(300) DEFAULT '',
    wb_time VARCHAR(50) DEFAULT '',
    window_time DATETIME NOT NULL,
    KEY idx_kw (keyword),
    KEY idx_ts (window_time),
    KEY idx_sent (sentiment),
    KEY idx_loc (location),
    KEY idx_gender (gender)
);

-- 8. 导入大学/专业种子数据
-- 运行: python data/rebuild_all.py

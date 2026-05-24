# -*- coding: utf-8 -*-
"""
Spark Streaming 微博版 v2 - Linux 版本
分析焦点: 帖子正文(情感/关键词) + 发帖量 + 用户分析
"""
import json, mysql.connector, sys, os

# 项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 加载配置
with open(os.path.join(project_root, "config.json"), "r", encoding="utf-8") as _f:
    CFG = json.load(_f)
MYSQL = CFG["mysql"]
KAFKA_BOOTSTRAP = CFG["kafka"]["bootstrap_servers"]
KAFKA_TOPIC = CFG["kafka"]["topic"]
DICT_PATH = os.path.join(project_root, "data", "gaokao_dict.txt")

# jieba 缓存目录设到项目目录
os.environ["JIEBA_CACHE_DIR"] = os.path.join(project_root, ".cache")

from snownlp import SnowNLP
import jieba
from ai_filter import is_quality_danmaku
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, udf, explode, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType
)

# 加载自定义词典
if os.path.exists(DICT_PATH):
    jieba.load_userdict(DICT_PATH)

STOP = {
    "的","了","是","我","你","他","她","它","们","这","那","不","也","就","都",
    "很","啊","吧","呢","吗","哦","嗯","哈","呀","在","有","和","与","或","但",
    "而","且","所","为","以","之","其","中","到","对","从","被","把","向","让",
    "给","上","下","说","看","想","要","会","能","可","知道","觉得",
    "一个","没有","什么","这个","那个","不是","就是","但是","因为","所以",
    "可以","应该","可能","已经","正在","还是","如果","虽然","然后","之后","或者",
    "非常","真的","这么","怎么","为什么","这样","那样","这些","那些","大家",
    "自己","时候","今天","明天","昨天","刚刚","一直","一样","有些",
    "https","http","web","www","com","cn","转发","回复","全文","分享",
    "原图","图片","视频","链接","网页","微博","weibo",
    # ===== 新增微博噪音词 =====
    "展开","超话","我的","话题","详情","查看","显示",
    "一下","一个","一条","一次","一点","一些","一天","一段",
    "发布","编辑","删除","收藏","点赞","评论","关注","粉丝",
    "网页链接","查看图片","查看原图","组图","长文","配图",
    "哈哈","哈哈哈","hhhh","呜呜","啊啊","呜呜呜","啊啊啊","设置","内容",
    "其实","不过","特别","一直","有点",
    "这里","那里","一种","每个",
    "看到","听到","想到","希望","期待",
    "能够","愿意","必须","一定",
    "通过","而且","最后",
    "目前","现在","当时","过去","最近","早就",
    "非常","比较","相当","极其","更加","越来越",
}


def analyze_sentiment(text):
    try:
        s = SnowNLP(str(text))
        score = s.sentiments
        if score > 0.6: return "positive"
        elif score < 0.4: return "negative"
        return "neutral"
    except:
        return "neutral"


def extract_keywords(text):
    import re
    # 去掉 @用户名(含 //@名字: 格式) 和 #话题#
    clean = re.sub(r'(//)?@\S+', '', str(text))
    clean = re.sub(r'#\S+#', '', clean)
    return [w for w in jieba.lcut(clean)
            if len(w) >= 2 and w not in STOP
            and '\u4e00' <= w[0] <= '\u9fff']


def batch_write(rows, table, columns):
    try:
        conn = mysql.connector.connect(**MYSQL)
        cur = conn.cursor()
        ph = ",".join(["%s"] * len(columns))
        cn = ",".join(columns)
        uc = ",".join([f"{c}=VALUES({c})" for c in columns if c != "id"])
        sql = f"INSERT INTO {table} ({cn}) VALUES ({ph}) ON DUPLICATE KEY UPDATE {uc}"
        cur.executemany(sql, rows)
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        print(f"  MySQL写入失败 [{table}]: {e}")


# ==================== Spark 配置 (Linux 版本) ====================
spark_tmp_dir = os.path.join(project_root, "..", "spark-tmp")
spark_checkpoint_dir = os.path.join(project_root, "..", "spark-checkpoint")
ivy_dir = os.path.join(project_root, "..", "spark-ivy")

spark = SparkSession.builder \
    .appName("GaokaoWeibo") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.default.parallelism", "2") \
    .config("spark.local.dir", spark_tmp_dir) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4") \
    .config("spark.jars.ivy", ivy_dir) \
    .config("spark.sql.streaming.checkpointLocation", spark_checkpoint_dir) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.addPyFile(os.path.join(os.path.dirname(__file__), "ai_filter.py"))

schema = StructType([
    StructField("type", StringType()),
    StructField("text", StringType()),
    StructField("keyword", StringType()),
    StructField("source", StringType()),
    StructField("user_name", StringType()),
    StructField("gender", StringType()),
    StructField("location", StringType()),
    StructField("topics", ArrayType(StringType())),
    StructField("mid", StringType()),
    StructField("wb_time", StringType()),
    StructField("send_time", LongType()),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("d"),
    col("timestamp").alias("kafka_ts")
).select("d.*", "kafka_ts")

stream_posts = parsed.filter(col("type") == "danmaku")

# ========== UDF ==========
sent_udf = udf(analyze_sentiment, StringType())
kw_udf = udf(extract_keywords, "array<string>")
quality_udf = udf(lambda t: bool(is_quality_danmaku(t)), "boolean")

stream_posts = stream_posts \
    .withColumn("sentiment", sent_udf(col("text"))) \
    .withColumn("keywords", kw_udf(col("text"))) \
    .withColumn("quality", quality_udf(col("text")))

stream_good = stream_posts.filter(col("quality") == True)

# ========== 聚合1: 帖子统计(按关键词,30s) ==========
agg_posts = stream_good \
    .withWatermark("kafka_ts", "5 minutes") \
    .groupBy(window("kafka_ts", "30 seconds"), col("keyword")) \
    .agg(count("*").alias("post_count"))


def write_posts(df, epoch):
    rows = [(str(r.window.start), r.keyword or "", int(r.post_count or 0))
            for r in df.collect()]
    if rows:
        batch_write(rows, "video_stats",
                    ["window_time", "keyword", "video_count"])
        print(f"  帖子统计: {len(rows)}行")


# ========== 聚合2: 每分钟发帖量 ==========
agg_rate = stream_good \
    .withWatermark("kafka_ts", "5 minutes") \
    .groupBy(window("kafka_ts", "1 minute")) \
    .agg(count("*").alias("count"))


def write_rate(df, epoch):
    rows = [(str(r.window.start), "", int(r["count"] or 0)) for r in df.collect() if r["count"]]
    if rows:
        batch_write(rows, "danmaku_per_minute", ["window_time", "video_title", "count"])
        print(f"  帖子速率: {len(rows)}行")


# ========== 聚合3: 关键词排行 ==========
agg_kw = stream_good \
    .select(explode(col("keywords")).alias("keyword")) \
    .groupBy("keyword") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc())


def write_kw(df, epoch):
    rows = [(r.keyword, int(r["count"] or 0)) for r in df.collect() if r.keyword]
    if rows:
        batch_write(rows, "keyword_ranking", ["keyword", "count"])
        print(f"  关键词: {len(rows)}行")


# ========== 聚合4: 情感分布(每分钟) ==========
agg_sent = stream_good \
    .withWatermark("kafka_ts", "5 minutes") \
    .groupBy(window("kafka_ts", "1 minute")) \
    .agg(
        count("*").alias("total"),
        count(when(col("sentiment") == "positive", 1)).alias("positive"),
        count(when(col("sentiment") == "neutral", 1)).alias("neutral"),
        count(when(col("sentiment") == "negative", 1)).alias("negative"),
    )


def write_sent(df, epoch):
    rows = [(str(r.window.start), int(r.positive or 0), int(r.neutral or 0),
             int(r.negative or 0)) for r in df.collect()]
    if rows:
        batch_write(rows, "sentiment_per_minute",
                    ["window_time", "positive", "neutral", "negative"])
        print(f"  情感: {len(rows)}行")


# ========== 聚合5: 活跃用户排行 ==========
agg_users = stream_good \
    .groupBy("user_name") \
    .agg(count("*").alias("post_count")) \
    .orderBy(col("post_count").desc())


def write_users(df, epoch):
    rows = []
    for r in df.collect():
        name = r.user_name or "匿名"
        cnt = int(r.post_count or 0)
        if name and cnt > 0:
            rows.append((name, cnt))
    if rows:
        try:
            conn = mysql.connector.connect(**MYSQL)
            cur = conn.cursor()
            cur.execute("DELETE FROM engagement_rate WHERE window_time='2026-01-01 00:00:00'")
            for name, cnt in rows[:30]:
                cur.execute(
                    "INSERT INTO engagement_rate(window_time,keyword,rate) "
                    "VALUES('2026-01-01 00:00:00',%s,%s) "
                    "ON DUPLICATE KEY UPDATE rate=VALUES(rate)",
                    (name, float(cnt)))
            conn.commit(); cur.close(); conn.close()
        except Exception as e:
            print(f"  活跃用户写入失败: {e}")
        print(f"  活跃用户: {len(rows[:30])}行")


# ========== 聚合6: 原始帖子明细 ==========
def write_raw(df, epoch):
    rows = []
    for r in df.collect():
        txt = str(r.text or "")[:280]
        rows.append((
            r.keyword or "", r.user_name or "",
            str(r.gender or ""), str(r.location or ""),
            r.sentiment or "",
            txt.replace("'", "").replace('"', ''),
            str(r.wb_time or ""), str(r.kafka_ts or ""),
        ))
    if rows:
        batch_write(rows, "raw_posts",
                    ["keyword", "user_name", "gender", "location",
                     "sentiment", "text_preview", "wb_time", "window_time"])
        print(f"  原始帖: {len(rows)}行")


# ========== 启动 ==========
print("=" * 50)
print("  🚀 Spark Streaming 微博话题分析 v2")
print(f"  Topic: {KAFKA_TOPIC}")
print(f"  分析: 正文/情感/关键词/发帖量/活跃用户")
print("=" * 50)

q1 = agg_posts.writeStream.outputMode("update").foreachBatch(write_posts).trigger(processingTime="30 seconds").start()
q2 = agg_rate.writeStream.outputMode("update").foreachBatch(write_rate).trigger(processingTime="30 seconds").start()
q3 = agg_kw.writeStream.outputMode("complete").foreachBatch(write_kw).trigger(processingTime="30 seconds").start()
q4 = agg_sent.writeStream.outputMode("update").foreachBatch(write_sent).trigger(processingTime="30 seconds").start()
q5 = agg_users.writeStream.outputMode("complete").foreachBatch(write_users).trigger(processingTime="30 seconds").start()
q6 = stream_good.writeStream.outputMode("append").foreachBatch(write_raw).trigger(processingTime="30 seconds").start()

spark.streams.awaitAnyTermination()
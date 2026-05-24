# 高考话题实时大数据分析系统

**选题：** 基于微博大数据的高考话题实时分析与可视化监控  
**数据源：** 微博 (Weibo s.weibo.com)  
**架构：** Kafka 4.3 KRaft → Spark Structured Streaming 4.1 → MySQL 8.0 → Streamlit

---

## 环境依赖

| 组件 | 版本 | 用途 |
|---|---|---|
| Python | 3.12+ | 爬虫 / Spark / 仪表盘 |
| Java JDK | 21 | Spark 运行环境 |
| Kafka | 4.3 (KRaft, 单节点) | 消息队列 |
| MySQL | 8.0 | 数据存储 |
| Hadoop winutils | 3.3.5 | Windows Spark 兼容层 |

Python 包（见 `requirements.txt`）：
```bash
pip install -r requirements.txt
```

## 项目结构

```
gaokao-project/
├── crawler/
│   └── weibo_crawler.py      # 微博爬虫（多线程、关键词评分、相关性过滤）
├── streaming/
│   ├── spark_consumer.py      # Spark 流处理（情感分析、分词、用户画像、6路聚合）
│   └── ai_filter.py           # 质量评分 + 高考相关性过滤 + TF-IDF 主题模型
├── dashboard/
│   └── app.py                 # Streamlit 仪表盘（15秒自动刷新）
├── data/
│   ├── init.sql               # MySQL 建表语句（7张表）
│   ├── rebuild_all.py         # 种子数据生成器（高校+专业+简称）
│   ├── gaokao_dict.txt        # jieba 自定义词典（578个高考相关词）
│   └── all_unis.txt           # 教育部官方高校名单（2919所）
├── config.json                # 统一配置文件（MySQL/Cookie/Kafka）— 已 gitignore
├── config.example.json        # 配置模板，复制为 config.json 后填入真实值
├── .gitignore
├── requirements.txt           # Python 依赖
└── README.md
```

## 快速部署

### 第一步：复制配置文件

```powershell
cd J:\Project\trae_projects\Bigdata\gaokao-project
copy config.example.json config.json
```

编辑 `config.json` 并填入你的凭证：
- `mysql.password` — MySQL root 密码
- `weibo_cookie` — 浏览器登录 weibo.com，打开开发者工具 (F12) -> Network -> 复制 Cookie 值

### 第二步：安装依赖

```powershell
pip install -r requirements.txt
```

### 第三步：初始化数据库与种子数据

```powershell
# 创建数据库和数据表
python -c "import mysql.connector,re,json;cfg=json.load(open('config.json','r',encoding='utf-8'));db=mysql.connector.connect(**cfg['mysql']);c=db.cursor();sql=open('data/init.sql','r',encoding='utf-8').read();sql=re.sub(r'--.*','',sql);[c.execute(s+';') for s in sql.split(';') if s.strip()];db.commit();c.close();db.close();print('数据库初始化完成')"

# 导入高校/专业种子数据
python data\rebuild_all.py
```

### 第四步：启动 Kafka

```powershell
J:\Kafka\kafka_2.13-4.3.0\bin\windows\kafka-server-start.bat `
  J:\Kafka\kafka_2.13-4.3.0\config\kraft\server.properties
```

### 第五步：启动系统（3 个终端）

**Terminal 1 — Spark Streaming:**
```powershell
cd J:\Project\trae_projects\Bigdata\gaokao-project
$env:HADOOP_HOME="J:\Hadoop"
$env:PYSPARK_PYTHON="python"
$env:PYSPARK_DRIVER_PYTHON="python"
Remove-Item -Recurse -Force "J:\Project\trae_projects\Bigdata\spark-checkpoint" -ErrorAction SilentlyContinue
python streaming\spark_consumer.py
```

**Terminal 2 — Weibo Crawler:**
```powershell
cd J:\Project\trae_projects\Bigdata\gaokao-project
python crawler\weibo_crawler.py
```

**Terminal 3 — Dashboard:**
```powershell
cd J:\Project\trae_projects\Bigdata\gaokao-project
streamlit run dashboard\app.py --server.port 8501
```

浏览器打开 `http://localhost:8501`. 仪表盘每 15 秒自动刷新。

## 数据库 (gaokao)

| 表名 | 说明 |
|---|---|
| `video_stats` | 帖子统计 (30s窗口, 按关键词) |
| `danmaku_per_minute` | 帖子每分钟速率 |
| `keyword_ranking` | jieba 分词关键词排行 |
| `sentiment_per_minute` | SnowNLP 情感分布 (正面/中性/负面) |
| `engagement_rate` | 活跃用户排行 |
| `university_ranking` | 教育部高校 (2919) + 简称 (176) + 专业 (565) |
| `raw_posts` | 帖子原始明细 (文本/情感/用户/性别/地区) |

## 数据流

```
微博 s.weibo.com
  | (HTTP 请求 + Cookie + 线程池并行)
weibo_crawler.py  -- 多关键词搜索, 相关性预筛, mid 去重
  | (Kafka Producer, gzip 压缩)
Kafka Topic: gaokao_topic
  | (Spark Structured Streaming)
spark_consumer.py
  |-- 高考相关性过滤 (ai_filter.py)
  |-- SnowNLP 情感分析 -> 正面 / 中性 / 负面
  |-- jieba 分词 + 自定义词典 (578 词)
  |-- 用户画像解析 (性别/地区 via weibo.com API)
  |-- 6 路 foreachBatch 聚合写入
  |
MySQL (7 张表)
  | (pd.read_sql + st.cache_data, 15s 刷新)
Streamlit 仪表盘
```

## 仪表盘模块

| 区域 | 模块 |
|---|---|
| KPI卡片 | 帖子总数 · 活跃话题 · 独立用户 · 关键词数 · 距高考天数 |
| 趋势 | 话题热度趋势(按关键词) · 帖子实时速率 |
| 内容分析 | 话题占比树图 · 关键词TOP20 · 情感仪表盘 |
| 情感 | 情感分布饼图 · 情感时序河流图 · 小时发帖分布 |
| 用户画像 | **性别分布饼图** · **地区分布TOP15** · 活跃用户TOP15 |
| 明细 | 关键词排行 · 最新帖子动态(含性别/地区) · 情感统计明细 |

## 关键技术点

- **爬虫：** 使用 `ThreadPoolExecutor(6)` 并行爬取15个关键词×2页，`threading.local()` 独立Session避免连接冲突，`mid` 去重
- **用户画像：** 调用 `weibo.com/ajax/profile/info?uid=` 获取性别(m/f)和地区(用户填写)，带内存缓存避免重复请求
- **情感分析：** SnowNLP，阈值 positive≥0.6, negative≤0.4, 其余中性
- **分词：** jieba + 2295词自定义词典（高考/大学/专业相关），过滤停用词
- **质量过滤：** TF-IDF + KMeans 聚类，剔除低质量/广告帖
- **窗口聚合：** 30s关键词统计 + 1min速率 + 1min情感分布

## 注意事项

1. **Cookie 有效期：** 爬虫使用的微博 Cookie 会过期（通常几天到几周），过期后需从浏览器重新导出，更新 `config.json` 中的 `weibo_cookie`
2. **评论不可获取：** 微博搜索结果显示 `showFeedComment=False`，所有评论API均返回空。帖子正文已足够 NLP 分析
3. **首次启动 Spark：** 会自动从 Maven 下载 Kafka 连接器 jar（仅一次）
4. **全部文件在 J 盘：** 不占用 C 盘空间
5. **Spark checkpoint：** 每次重启建议删除 `spark-checkpoint/` 目录避免偏移量冲突

## 大作业信息

- **实时大数据分析 — 选题4：** 自由选题 — 关于高中生的调研与实时大数据分析
- **数据可视化技术 — 题目4：** 自由选题 — 可视化展示
- **选题标题：** 基于微博大数据的高考话题实时分析与可视化监控

"""框架配置：每个变量通过 os.getenv(同名变量, 默认值) 读取。

本文件是唯一配置文件，不加载项目或示例目录中的配置模块。
优先级从高到低：同名环境变量 > CLI 运行参数 > Crawler.custom_settings >
本文件默认值。环境变量使用原名，不加前缀。
例如平台注入 SEED_CONCURRENCY=12 后，即使 CLI 指定其他并发数也以 12 为准。
时间配置的单位均为秒；列表/字典类环境变量使用 JSON，布尔值使用 true/false 或 1/0。
"""

import os

from .utils.environment import decode_environment_value as _decode


# 纯默认值单独保存，避免首次导入时的环境配置成为后续 Crawler 的默认值。
# 每个 Crawler 仍保存自己的配置快照；所有 os.getenv 的回退值均来自这里。
_defaults = {
    'REDIS_URL': None,
    'MONGO_URI': None,
    'MONGO_DATABASE': None,
    'SEED_CONCURRENCY': 8,
    'REQUEST_CONCURRENCY': 16,
    'REQUEST_TIMEOUT': 30.0,
    'REQUEST_MAX_RETRIES': 3,
    'REQUEST_RETRY_BACKOFF': 0.5,
    'REQUEST_RETRY_BACKOFF_MAX': 30.0,
    'RETRY_HTTP_STATUS_CODES': [408, 429, 500, 502, 503, 504],
    'RETRY_EXCEPTIONS': [],
    'RETRY_AFTER_MAX': 300.0,
    'REQUEST_INTERVAL': 0.0,
    'SEED_MAX_EXECUTIONS': 3,
    'LEASE_TIMEOUT': 120.0,
    'LEASE_RENEW_INTERVAL': 30.0,
    'RECOVERY_SCAN_INTERVAL': 10.0,
    'DEDUP_TTL': 604800,
    'REQUEST_BUFFER_LIMIT': 1000,
    'SEED_REQUEST_LIMIT': 1000,
    'MONGO_UPSERT_KEYS': None,
    'LOG_SOURCE': None,
    'LOG_TOPIC': None,
    'ITEM_BATCH_SIZE': 100,
    'ITEM_FLUSH_INTERVAL': 1.0,
    'ITEM_MAX_RETRIES': 3,
    'ITEM_RETRY_TIMEOUT': 300.0,
    'ITEM_RETRY_BACKOFF': 0.1,
    'ITEM_RETRY_BACKOFF_MAX': 5.0,
    'ITEM_BUFFER_LIMIT': 2000,
    'DEFAULT_PROXY': None,
    'PROXY_PROVIDER': None,
    'DOWNLOADER': 'spider_man.network.downloader.http_downloader.HTTPDownloader',
    'ITEM_PIPELINES': ['spider_man.pipelines.mongo_pipeline.MongoPipeline'],
    'ALLOW_CONSOLE_ONLY': False,
    'SHUTDOWN_TIMEOUT': 60.0,
    'POLL_INTERVAL': 0.2,
    'LOG_LEVEL': 'INFO',
    'LOG_JSON': True,
    'METRICS_INTERVAL': 10.0,
}


def _value(name, value):
    return _decode(name, value, _defaults[name]) if isinstance(value, str) else value

# -------------------- 数据库连接 --------------------

# Redis 连接地址，启动 Worker 前必须配置；支持 redis:// 和 rediss://。
# 例如 redis://127.0.0.1:6379/0，可在地址中指定认证信息和逻辑数据库编号。
REDIS_URL = _value("REDIS_URL", os.getenv("REDIS_URL", _defaults["REDIS_URL"]))

# MongoDB 连接地址，启动 Worker 前必须配置；支持 mongodb:// 和 mongodb+srv://。
MONGO_URI = _value("MONGO_URI", os.getenv("MONGO_URI", _defaults["MONGO_URI"]))

# MongoDB 数据库名，启动 Worker 前必须配置；集合名由 LOG_SOURCE 和 LOG_TOPIC 拼接。
MONGO_DATABASE = _value("MONGO_DATABASE", os.getenv("MONGO_DATABASE", _defaults["MONGO_DATABASE"]))


# -------------------- 并发与 HTTP 请求 --------------------

# 单个 Worker 同时执行的 Seed 数量；多个 Worker 的容量分别计算。
# 兼容环境变量 SPIDER_THREAD_NUM；原名 SEED_CONCURRENCY 优先。
# 此兼容变量只控制 Seed 并发，请求并发仍由 REQUEST_CONCURRENCY 独立控制。
SEED_CONCURRENCY = _value("SEED_CONCURRENCY", os.getenv("SEED_CONCURRENCY", os.getenv("SPIDER_THREAD_NUM", _defaults["SEED_CONCURRENCY"])))

# 单个 Worker 同时调用下载器的上限；同一 Seed 内的请求仍按产出顺序执行。
REQUEST_CONCURRENCY = _value("REQUEST_CONCURRENCY", os.getenv("REQUEST_CONCURRENCY", _defaults["REQUEST_CONCURRENCY"]))

# 默认 HTTP 连接及读取超时，可被 Request.timeout 覆盖。
# 读取超时指等待网络数据的超时，不是整个 Seed 或整个下载过程的总时限。
REQUEST_TIMEOUT = _value("REQUEST_TIMEOUT", os.getenv("REQUEST_TIMEOUT", _defaults["REQUEST_TIMEOUT"]))

# 每个逻辑请求在首次尝试之外的最大重试次数；3 表示最多尝试 4 次，0 表示不重试。
# 默认只允许 GET/HEAD/OPTIONS 重试，其他方法需 Request.retryable=True；
# Request.max_retries 可覆盖次数，同一 Seed 执行中重复派生同一请求共用预算。
REQUEST_MAX_RETRIES = _value("REQUEST_MAX_RETRIES", os.getenv("REQUEST_MAX_RETRIES", _defaults["REQUEST_MAX_RETRIES"]))

# 请求重试的指数退避基数，首次等待约为此值，带 0.8～1.2 倍随机抖动；0 表示不退避。
REQUEST_RETRY_BACKOFF = _value("REQUEST_RETRY_BACKOFF", os.getenv("REQUEST_RETRY_BACKOFF", _defaults["REQUEST_RETRY_BACKOFF"]))

# 请求指数退避（含抖动）的最大等待时间；服务端 Retry-After 使用独立上限。
REQUEST_RETRY_BACKOFF_MAX = _value("REQUEST_RETRY_BACKOFF_MAX", os.getenv("REQUEST_RETRY_BACKOFF_MAX", _defaults["REQUEST_RETRY_BACKOFF_MAX"]))

# 默认 HTTP 下载器将这些错误状态码视为可重试；仍受请求方法与次数预算限制。
RETRY_HTTP_STATUS_CODES = _value("RETRY_HTTP_STATUS_CODES", os.getenv("RETRY_HTTP_STATUS_CODES", _defaults["RETRY_HTTP_STATUS_CODES"]))

# 自定义下载器额外允许重试的异常类完整路径，例如 ["builtins.OSError"]。
# 默认下载器的网络异常已有分类；未声明的普通异常不会自动重试。
RETRY_EXCEPTIONS = _value("RETRY_EXCEPTIONS", os.getenv("RETRY_EXCEPTIONS", _defaults["RETRY_EXCEPTIONS"]))

# 服务端 Retry-After 建议等待时间的上限；实际等待取它与请求退避时间的较大值。
RETRY_AFTER_MAX = _value("RETRY_AFTER_MAX", os.getenv("RETRY_AFTER_MAX", _defaults["RETRY_AFTER_MAX"]))

# 单个 Worker 开始两次下载器调用的最小间隔，覆盖该 Worker 内所有 Seed；0 表示不限速。
# 这是当前实例的间隔控制，多个 Worker 之间不共享此限制。
REQUEST_INTERVAL = _value("REQUEST_INTERVAL", os.getenv("REQUEST_INTERVAL", _defaults["REQUEST_INTERVAL"]))


# -------------------- Seed 执行、租约与恢复 --------------------

# 同一 Seed 的总执行次数上限，包含首次执行；恢复或 Seed 重试共用 Redis 中的预算。
# 仅对允许恢复的故障重新执行，永久错误会直接进入失败记录。
SEED_MAX_EXECUTIONS = _value("SEED_MAX_EXECUTIONS", os.getenv("SEED_MAX_EXECUTIONS", _defaults["SEED_MAX_EXECUTIONS"]))

# 已领取 Seed 的租约有效期；到期未续租的任务可被回收，正常续租可延长执行时间。
LEASE_TIMEOUT = _value("LEASE_TIMEOUT", os.getenv("LEASE_TIMEOUT", _defaults["LEASE_TIMEOUT"]))

# 活跃 Seed 的续租和 Worker 心跳间隔，必须小于 LEASE_TIMEOUT。
LEASE_RENEW_INTERVAL = _value("LEASE_RENEW_INTERVAL", os.getenv("LEASE_RENEW_INTERVAL", _defaults["LEASE_RENEW_INTERVAL"]))

# 扫描租约过期任务及到期延迟重试的间隔。
RECOVERY_SCAN_INTERVAL = _value("RECOVERY_SCAN_INTERVAL", os.getenv("RECOVERY_SCAN_INTERVAL", _defaults["RECOVERY_SCAN_INTERVAL"]))

# 稳定 Seed 身份的去重窗口，默认 7 天；窗口过期后相同身份可能再次入队。
# 必须 >= LEASE_TIMEOUT * SEED_MAX_EXECUTIONS + ITEM_RETRY_TIMEOUT，覆盖恢复预算。
DEDUP_TTL = _value("DEDUP_TTL", os.getenv("DEDUP_TTL", _defaults["DEDUP_TTL"]))

# 单个 Seed 内回调迭代器栈的最大深度（含根回调），用于限制嵌套 yield Request。
# 不是请求队列总量；深层流程可拆成子 Seed。
REQUEST_BUFFER_LIMIT = _value("REQUEST_BUFFER_LIMIT", os.getenv("REQUEST_BUFFER_LIMIT", _defaults["REQUEST_BUFFER_LIMIT"]))

# 单次 Seed 执行允许追踪的不同逻辑 Request 数量上限，限制累计请求预算与内存占用。
# 重复的 request_id 共用预算；超过上限时应将分页或后续工作拆成子 Seed。
SEED_REQUEST_LIMIT = _value("SEED_REQUEST_LIMIT", os.getenv("SEED_REQUEST_LIMIT", _defaults["SEED_REQUEST_LIMIT"]))


# -------------------- MongoDB 写入策略 --------------------

# upsert 使用的业务唯一字段，例如 ["user_id", "post_id"]，需按字段顺序匹配有效唯一索引。
# None 时自动选择唯一的业务唯一索引；多个索引需显式指定，无业务索引则 insert。
# MongoDB 默认 _id 索引不自动作为业务键，按 _id 更新需显式填写 ["_id"]。
# 业务索引需在 on_start 中创建或提前建好；默认实现不支持稀疏、部分或带 collation 的业务索引。
MONGO_UPSERT_KEYS = _value("MONGO_UPSERT_KEYS", os.getenv("MONGO_UPSERT_KEYS", _defaults["MONGO_UPSERT_KEYS"]))

# 写入集合的来源部分；未配置时使用当前 Crawler 的 project。
# 与 LOG_TOPIC 用下划线拼接，例如 LOG_SOURCE=tiktok、LOG_TOPIC=user_list → tiktok_user_list。
# 只控制 MongoDB 集合，不改变 Redis 队列标识；显式空字符串会报错。
LOG_SOURCE = _value("LOG_SOURCE", os.getenv("LOG_SOURCE", _defaults["LOG_SOURCE"]))

# 写入集合的主题部分；未配置时使用当前 Crawler 的 task。
# 支持业务代码提供默认值，同名环境变量优先；显式空字符串会报错。
LOG_TOPIC = _value("LOG_TOPIC", os.getenv("LOG_TOPIC", _defaults["LOG_TOPIC"]))


# -------------------- Item 缓冲与存储重试 --------------------

# 每次提交给 Pipeline 的批次条数上限，必须 <= ITEM_BUFFER_LIMIT。
ITEM_BATCH_SIZE = _value("ITEM_BATCH_SIZE", os.getenv("ITEM_BATCH_SIZE", _defaults["ITEM_BATCH_SIZE"]))

# 收到批次首条 Item 后等待凑批的最大时间；满批或 flush/停机唤醒会提前提交。
ITEM_FLUSH_INTERVAL = _value("ITEM_FLUSH_INTERVAL", os.getenv("ITEM_FLUSH_INTERVAL", _defaults["ITEM_FLUSH_INTERVAL"]))

# 每条 Item 在每个 Pipeline 阶段首次调用之外的额外存储重试次数。
# 仅重试可恢复存储错误；3 表示最多调用 4 次，0 表示不重试。
ITEM_MAX_RETRIES = _value("ITEM_MAX_RETRIES", os.getenv("ITEM_MAX_RETRIES", _defaults["ITEM_MAX_RETRIES"]))

# 一个批次进入处理后、跨所有 Pipeline 阶段累计的存储重试时间预算。
# 不包含排队/攒批等待，也不强制中断正在执行的存储调用；扩展需自行设置连接超时。
ITEM_RETRY_TIMEOUT = _value("ITEM_RETRY_TIMEOUT", os.getenv("ITEM_RETRY_TIMEOUT", _defaults["ITEM_RETRY_TIMEOUT"]))

# 首次额外存储重试的等待时间，后续指数翻倍，也受剩余重试时间预算限制；0 表示不退避。
ITEM_RETRY_BACKOFF = _value("ITEM_RETRY_BACKOFF", os.getenv("ITEM_RETRY_BACKOFF", _defaults["ITEM_RETRY_BACKOFF"]))

# 单次存储重试退避的最大等待时间。
ITEM_RETRY_BACKOFF_MAX = _value("ITEM_RETRY_BACKOFF_MAX", os.getenv("ITEM_RETRY_BACKOFF_MAX", _defaults["ITEM_RETRY_BACKOFF_MAX"]))

# 单个 Worker 尚未确认完成的 Item 总量上限，包含等待和正在处理的 Item。
# 满额时阻塞新提交，直到已有 Item 完成或失败后释放容量。
ITEM_BUFFER_LIMIT = _value("ITEM_BUFFER_LIMIT", os.getenv("ITEM_BUFFER_LIMIT", _defaults["ITEM_BUFFER_LIMIT"]))


# -------------------- 下载器、代理与 Pipeline 扩展 --------------------

# 默认代理：None/False 表示直连，可填代理 URL 或 {"http": URL, "https": URL}。
# 选择优先级：Request.proxies > PROXY_PROVIDER > DEFAULT_PROXY；不读取系统环境代理。
DEFAULT_PROXY = _value("DEFAULT_PROXY", os.getenv("DEFAULT_PROXY", _defaults["DEFAULT_PROXY"]))

# 代理供应器类的完整路径；None 表示不启用，扩展实现 get_proxy(context)。
# 配置供应器后由其决定默认代理；返回 None/False 时直连，不再回退 DEFAULT_PROXY。
PROXY_PROVIDER = _value("PROXY_PROVIDER", os.getenv("PROXY_PROVIDER", _defaults["PROXY_PROVIDER"]))

# 下载器类的完整路径；实例通过构造器接收 Crawler，实现 download(request) 与 close()。
DOWNLOADER = _value("DOWNLOADER", os.getenv("DOWNLOADER", _defaults["DOWNLOADER"]))

# 按顺序执行的 Pipeline 类完整路径列表，不能为空；默认使用 MongoDB 存储。
# 正常运行至少需要一个 required=True 且 persistent=True 的 Pipeline，确认后才能 ACK。
ITEM_PIPELINES = _value("ITEM_PIPELINES", os.getenv("ITEM_PIPELINES", _defaults["ITEM_PIPELINES"]))

# 允许仅使用 Console 等非持久化 Pipeline 运行，仅用于本地调试。
# 此时 ACK 只代表处理完成；仍需配置 Redis/MongoDB 连接。
ALLOW_CONSOLE_ONLY = _value("ALLOW_CONSOLE_ONLY", os.getenv("ALLOW_CONSOLE_ONLY", _defaults["ALLOW_CONSOLE_ONLY"]))


# -------------------- 运行管理、日志与指标 --------------------

# 停止领取新任务后，等待在途 Seed 完成的宽限时间；超时任务保留租约供后续恢复。
# Pipeline 关闭等待也使用此值，因此不是整个 close() 的硬性总时限。
SHUTDOWN_TIMEOUT = _value("SHUTDOWN_TIMEOUT", os.getenv("SHUTDOWN_TIMEOUT", _defaults["SHUTDOWN_TIMEOUT"]))

# Worker 主循环无立即可处理进展时的轮询间隔，影响空队列响应与停止响应速度。
POLL_INTERVAL = _value("POLL_INTERVAL", os.getenv("POLL_INTERVAL", _defaults["POLL_INTERVAL"]))

# 日志最低级别，可选 DEBUG、INFO、WARNING、ERROR、CRITICAL。
LOG_LEVEL = _value("LOG_LEVEL", os.getenv("LOG_LEVEL", _defaults["LOG_LEVEL"]))

# True 输出 JSON 结构化日志；False 输出 Python 字典的文本表示。
LOG_JSON = _value("LOG_JSON", os.getenv("LOG_JSON", _defaults["LOG_JSON"]))

# 定期输出指标、队列和缓冲快照的间隔，同时用于故障率、积压与 Worker 失联检查。
METRICS_INTERVAL = _value("METRICS_INTERVAL", os.getenv("METRICS_INTERVAL", _defaults["METRICS_INTERVAL"]))


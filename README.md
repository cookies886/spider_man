# spider_man

Python 分布式爬虫 SDK。通过 `Crawler`、`@app.request`、`@app.parse` 和 `yield` 编写采集任务，由框架管理 Redis 种子队列、HTTP 下载、请求重试和 MongoDB 存储。

- 多个 Worker 共享 Redis ZSet，支持优先级、租约续期、崩溃恢复和失败重放。
- 请求、数据和子 Seed 全部确认后才完成父 Seed；Item 缓冲有上限，存储故障时暂停新领取。
- MongoDB 根据业务唯一索引执行 upsert，没有业务唯一键时直接插入。
- 提供下载器、代理提供器、Pipeline 扩展，以及日志、指标和命令行工具。

## 安装

需要 Python 3.10+ 和 Git。运行任务还需要可访问的 Redis 与 MongoDB。

从 GitHub 安装指定版本：

```bash
python -m pip install 'spider-man @ git+https://github.com/cookies886/spider_man.git@v0.1.0'
```

业务项目的 `requirements.txt` 使用同一固定 tag：

```text
spider-man @ git+https://github.com/cookies886/spider_man.git@v0.1.0
```

然后安装项目依赖：

```bash
python -m pip install -r requirements.txt
```

包名称为 `spider-man`，Python 导入名称为 `spider_man`。版本通过 Git tag 标识；升级时更新依赖中的 tag，并验证业务任务。

使用私有仓库或私有派生仓库时，先配置 Git 凭据管理器或 SSH 密钥，让运行环境能够访问仓库。SSH 安装格式如下；私有派生仓库应替换为自己的仓库地址：

```bash
python -m pip install 'spider-man @ git+ssh://git@github.com/cookies886/spider_man.git@v0.1.0'
```

Token 交给凭据管理器或 CI 的凭据注入机制管理，避免将明文 Token 写入 Git URL、`requirements.txt` 或业务源码。

## 编写任务

业务项目中新建 `task.py`。下面示例使用仓库提供的本地模拟 HTTP 接口，替换 URL 和解析逻辑即可接入自己的数据源。

```python
from spider_man import Crawler, Request, Seed, Item

app = Crawler(
    project="demo",
    task="user_post",
    custom_settings={"MONGO_UPSERT_KEYS": ["user_id", "post_id"]},
)


@app.on_start
def indexes():
    app.mongo.create_index(
        app.collection_name, ["user_id", "post_id"], unique=True
    )


@app.request
def request(seed):
    if not seed.user_id:
        raise ValueError("user_id is required")
    yield Request(
        "http://127.0.0.1:18080/posts",
        params={"user_id": seed.user_id, "page": seed.page or 1},
        seed=seed,
    )


@app.parse
def parse(result):
    body = result.json()
    for post in body["posts"]:
        yield Item(data=post, seed=result.seed)
    if body.get("next_page"):
        yield Seed({"user_id": result.seed.user_id, "page": body["next_page"]})


if __name__ == "__main__":
    app.start()
```

`Seed` 支持字典读取、属性读取和 `to_dict()`。框架控制字段与业务数据分开；业务字段与控制字段同名时使用 `seed["field"]` 读取。`Request`、`Item` 保留源 Seed，解析函数可以继续产出 Request、Item 或后续 Seed。多个解析回调通过 `@app.parse("detail")` 注册，再设置 `Request(callback="detail")`。

跨任务派发可以 yield `Seed(data, project="demo", task="detail")`，或调用 `app.enqueue(seed, project="demo", task="detail")`。回调内派发会等待此前 Item 的存储确认；父 Seed 等待子 Seed 入队确认，无需等待子任务执行完成。

## 配置与启动

框架默认值集中在 [spider_man/setting.py](spider_man/setting.py)，业务侧通过同名环境变量配置，也可在创建 Crawler 时提供 `custom_settings` 字典。配置优先级为：**同名环境变量 > 允许的 CLI 运行参数 > `custom_settings` > 框架默认值**。

启动前提供三个连接配置，以下地址用于本地开发服务：

```bash
export REDIS_URL='redis://127.0.0.1:6379/0'
export MONGO_URI='mongodb://127.0.0.1:27017'
export MONGO_DATABASE='spider_man_demo'
```

新建 `seeds.jsonl`，每行一个业务数据字典：

```json
{"user_id":"demo_user","page":1}
```

在 `task.py` 所在目录运行：

```bash
spider-man check task:app
spider-man seed task:app seeds.jsonl
spider-man run task:app --until-idle
spider-man status task:app
```

`check` 检查配置、连接和已有索引，建索引钩子在 `run` 时执行。持续运行 Worker 时省略 `--until-idle`；也可直接执行 `python task.py`。`python -m spider_man` 提供相同命令入口。

常用配置：

| 配置 | 默认值或行为 |
| --- | --- |
| `SEED_CONCURRENCY` | 每 Worker 同时执行 8 个 Seed |
| `REQUEST_CONCURRENCY` | 每 Worker 最多 16 个在途 HTTP 请求 |
| `REQUEST_TIMEOUT` | 30 秒 |
| `REQUEST_MAX_RETRIES` | 首次请求之外，最多额外重试 3 次 |
| `MONGO_UPSERT_KEYS` | 自动识别业务唯一索引，也可声明字段列表 |
| `LOG_SOURCE` / `LOG_TOPIC` | 默认均为 `None`；分别回退到 `project` / `task` |
| `ITEM_BATCH_SIZE` / `ITEM_FLUSH_INTERVAL` | 100 条或 1 秒触发提交 |
| `ITEM_BUFFER_LIMIT` | 最多 2000 条未确认 Item |

MongoDB 集合名为 `LOG_SOURCE + "_" + LOG_TOPIC`，每个未配置部分独立回退。例如 `Crawler("demo", "user_post")` 默认写入 `demo_user_post`；设置 `LOG_SOURCE=archive`、`LOG_TOPIC=posts` 后写入 `archive_posts`。这两项只控制集合名，Redis 队列始终按 Crawler 的 `project/task` 命名，例如 `{demo:user_post}:todo`。

数组、字典使用 JSON 环境值，例如 `MONGO_UPSERT_KEYS='["user_id","post_id"]'`；布尔值支持 `true/false`、`1/0`。非法配置在创建实例时明确报错。每个 Crawler 保存独立、不可变的配置快照。

业务自定义环境变量可用 `os.getenv()`，或从实例环境快照中读取：

```python
api_key = app.settings.getenv("TASK_API_KEY")
batch_size = app.settings.getenv("API_BATCH_SIZE", default=20, cast=int)
```

`custom_settings` 也可声明大写业务默认值，同名环境变量按其默认类型覆盖。`app.settings.getenv()` 按精确名称读取实例创建时的环境，不自动添加前缀或回退到 `custom_settings`。

## MongoDB 存储与扩展

索引检查发生在 `on_start` 之后、消费之前。只有一个普通业务唯一索引时自动采用；多个时必须通过 `MONGO_UPSERT_KEYS` 明确选择。显式匹配键需要对应的有效唯一索引；稀疏、部分或带特殊排序规则的业务唯一索引需要专门适配。

upsert 合并本次提供的顶层业务字段，缺失字段保留，显式 `None` 写入 `null`；嵌套对象整字段替换。只有 MongoDB 默认 `_id` 索引时仍使用 insert，显式声明 `MONGO_UPSERT_KEYS=["_id"]` 才按该身份更新。框架时间保存在 `_spider_man.created_at` / `updated_at`，业务 Item 不得占用 `_spider_man`。

`app.redis`、`app.mongo` 提供直接数据库访问。需要框架追踪存储确认、重试和指标的采集数据使用 Item/Pipeline；派发任务使用 `enqueue` 或 yield Seed。

以下配置接受完整类路径：

```python
custom_settings = {
    "DOWNLOADER": "spider_man.network.downloader.http_downloader.HTTPDownloader",
    "ITEM_PIPELINES": ["spider_man.pipelines.mongo_pipeline.MongoPipeline"],
}
```

下载器实现 `download(request)` 与 `close()`。Pipeline 支持 `open`、`process_item` / `process_batch`、`flush`、`close`；持久化步骤须逐条返回 `PipelineResult`，批次可以返回每条结果或异常。明确丢弃使用 `DropItem("业务原因")`。可参考 [BasePipeline](spider_man/pipelines/base_pipeline.py) 与 [ProxyProvider](spider_man/network/proxy_pool/proxy_provider.py)。生产配置至少需要一个必需的持久化 Pipeline；本地 Console 调试须显式启用 `ALLOW_CONSOLE_ONLY=True`。

## 多 Worker 与失败处理

多个 Worker 使用相同任务代码、Redis 配置和 `project/task` 即可共同消费。领取、续租、ACK 和失败迁移校验租约归属；Worker 中断后，过期任务由运行中的 Worker 回收。

框架提供至少一次执行语义。写库成功后、ACK 前退出会导致任务重跑，有唯一键时通过 upsert 控制重复；没有业务唯一键时，重跑或不确定写入结果的重试可能追加重复记录。多个存储目标之间不提供原子事务。

默认重试可重复读取的请求和配置中的临时 HTTP 错误；POST 等方法需显式声明可重试。普通解析错误、非法 Item 和耗尽重试预算的任务保留到 failed，便于定位与重放。查看与重放示例：

```bash
spider-man failed task:app --error-type http_status --limit 20
spider-man retry task:app --seed-id '<failed-seed-id>' --limit 1
```

SIGINT/SIGTERM 停止新领取并等待在途任务；超过停机时限的未确认工作保留恢复。状态和指标用于运行观测，不作为精确业务账本。

## 开发与本地示例

克隆仓库后安装开发依赖：

```bash
git clone https://github.com/cookies886/spider_man.git
cd spider_man
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

在仓库根目录启动合成数据源：

```bash
python -m examples.demo_server
```

另开终端，激活同一虚拟环境并配置本地 Redis、MongoDB 连接后运行：

```bash
spider-man check examples.demo:app
spider-man seed examples.demo:app examples/seeds.jsonl
spider-man run examples.demo:app --until-idle
spider-man status examples.demo:app
```

默认示例采集三页合成数据，生成六条记录，使用复合唯一键存入 `demo_user_post`。示例源码位于 [examples/demo.py](examples/demo.py)，模拟接口位于 [examples/demo_server.py](examples/demo_server.py)。

运行测试：

```bash
python -m pytest -q
SPIDER_MAN_TEST_MONGO_URI='mongodb://127.0.0.1:27017' python -m pytest -q
```

Redis 集成测试自行启动隔离的 `redis-server`，需要本机已安装该命令。真实 MongoDB 测试在指定实例创建随机 `spider_man_test_*` 数据库，完成后删除；请使用独立测试实例。未设置测试 URI 时，真实 MongoDB 用例跳过。

发布新版本时，同步 `pyproject.toml` 与包内版本号，执行测试，再创建对应的 `v版本号` tag。业务依赖固定到经过验证的 tag，部署环境保持业务代码与 SDK 版本一致。

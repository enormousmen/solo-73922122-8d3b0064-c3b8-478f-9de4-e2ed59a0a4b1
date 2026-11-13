# 机构库 → 馆藏目录采集

把机构库 OAI-PMH（`oai_dc`）的新增、修改、撤回同步为一份可导入离线发现系统的
馆藏目录 XML。FastAPI + httpx + SQLite，附本地演示 OAI 源，Docker Compose 一条命令启动。

## 快速启动

```bash
docker compose up --build -d
```

* 采集页：<http://localhost:8000/>（采集、下载、进度、错误、续采）
* 本地演示 OAI-PMH 源：<http://localhost:8001/>（修改/撤回/凭据失效操作面板）
* 源端点：<http://localhost:8001/oai>（`ListRecords&metadataPrefix=oai_dc`）

```bash
docker compose logs -f catalog-web    # 观察采集中/发布失败等日志
docker compose down                   # 停止（数据保留在命名卷）
docker compose down -v                # 连数据一起清空
```

页面操作顺序（也可直接看页面底部"演示脚本"）：

1. **首次全量**：选本轮截止日（默认今天）→ 开始采集。无水位时 `ListRecords`
   不带 `from`，收齐后生成目录并发布，水位 = 截止日。
2. **同日更新 / 撤回**：在 8001 演示源修改标题或撤回一条记录（源端 datestamp
   刷新为今天，也可在面板指定变更日期）→ 再次采集。窗口为
   `from=上次成功截止日`（含当天），按 `identifier` 更新或删除。未出现的记录
   不动，**不以缺席推断删除**。
3. **分页凭据失效**：演示源上点“下一次凭据请求失效”→ 采集端起续页时收到
   `badResumptionToken`，立即清空本轮暂存、回到原窗口第一页重采，水位不推进。
   页面的“凭据重置 N 次”和橙色提示会记录此事件。
4. **放弃本轮（核心修复）**：采到一半（暂停中、采集中）或发布失败的轮次都可
   点“放弃本轮”。放弃在数据库里条件翻转状态并**清空该轮暂存**，其增改删绝不
   会进入后续任何目录；已发布目录与水位保持不变。放弃与继续同时提交时只接纳
   一种；放弃成功后迟到的采集/发布动作写不进也发布不了，重启后同样有效。
5. **跨截止日演示**：演示源“批量改期”到 13 日并全量发布；把一条记录的改名/
   撤回日期指定为 15 日，开截止 15 日的窗口采到暂停后放弃；再开截止 14 日的
   窗口完成发布 —— 下载目录不含任何 15 日变更。
6. **发布前后退出**：采集页“发布故障演练”选 `crash_after_publish` 或
   `crash_before_commit`，再跑一轮 → 进程退出。执行
   `docker compose restart catalog-web` 后，页面水位与下载文件仍是上一版完整
   结果；`crash_after_publish` 的轮次显示“发布失败·可重试”，点重试即可发布
   新版本，无需重采。

命令行等价演示（容器内）：

```bash
docker compose exec catalog-web python -m scripts.demo_flow
```

## 放弃隔离是怎么做到的

* 每一轮采到的记录先写入该轮专属的 `run_records` 暂存表，采集过程绝不触碰
  已发布快照 `records`；页面“本轮暂存变更”读的就是这张表。
* 发布时以 **最后成功发布的 `records` + 本轮暂存的增改删** 生成目录，并在单个
  SQLite 事务里完成“暂存覆盖 → publications 落库 → 轮次 completed”。事务带
  `status='publishing'` 条件，轮次若已被放弃则整体回滚。
* 放弃 = 条件 `UPDATE ... WHERE status IN (...)` 翻转状态 + 同事务删除该轮
  暂存。所有状态推进（继续、重试发布、进度写入、发布提交）都是条件更新，
  因此放弃与继续并发时只有一个动作能赢；放弃成功后的迟到处理在循环检查、
  暂存写入、发布提交三道关口都会被拒绝。
* 未在本轮返回的记录保持已发布快照原样（含既有 tombstone，永久保留），
  绝不以缺席推断删除。

## 行为设计

| 需求 | 实现 |
| --- | --- |
| 固定地址读取 oai_dc | `OAI_ENDPOINT` 固定，`ListRecords` + `resumptionToken` |
| 源按日标记变更、删除标记永久保留 | `records.identifier` 唯一，`deleted=1` 只置位不删除；重采/再版才改 |
| 首次全量，此后从上次成功截止日含当天重采 | `runs.mode=full/incremental`；`window_from=watermark`，`from/until` 均含当天 |
| 网页选择本轮截止日 | 首页日期表单 → `runs.window_until`；演示源可指定变更日期/批量改期 |
| 按 identifier 更新或删除 | 每页记录 UPSERT 进**本轮暂存表**；删除头写 tombstone，发布时覆盖 |
| 不以缺席推断删除 | 发布合并只处理本轮暂存 + 已发布快照，从不扫描本地记录做删除 |
| 放弃轮次的增改删不进后续目录 | 放弃在同一事务翻转状态并清空 `run_records`；已发布快照/水位不动 |
| 新一轮只在最后成功发布状态上应用本轮变更 | 发布合并视图 = `records`（最后发布快照）叠加本轮 `run_records` |
| 放弃与继续同时提交只接纳一种 | 全部状态推进为条件 UPDATE；仲裁在 SQLite 事务内完成，并发只赢一个 |
| 放弃成功后迟到处理不写入/发布 | 循环状态检查 + 暂存写入校验 + 发布事务 `status='publishing'` 三重守卫 |
| 重启后放弃仍然有效 | 启动把 running 转 paused，并清理已结束轮次残留暂存；canceled 不可复活 |
| 同期只采一轮 | `runs` 上的部分唯一索引 + 应用层检查；未解决的轮次挡住新轮次 |
| 保存分页进度、断流/重启可续采 | 每页提交暂存与 `resumption_token`（SQLite 事务）；启动把 `running` 转 `paused` |
| badResumptionToken 重采原窗口、不推进水位 | 收到该错误即清空本轮暂存、重置页码/token 从原窗口第一页重采；水位只在发布时写 |
| 收齐后生成含全部有效记录的 XML | 合并视图 `where deleted=0` 全量入目录，`<catalog watermark recordCount>` |
| 文件与水位一起发布 | 单事务：暂存覆盖 + `publications` 行（文件路径+水位+sha256）+ 轮次完成 |
| 生成或发布失败保留旧目录 | 先构造 XML、写临时文件、原子 `os.replace`，最后落库事务；任一步失败旧行不动 |
| 发布中退出后重启，内容与水位对应同一完整结果 | 唯一文件名+原子改名；未落库的孤儿文件启动时清理；`publishing` 按有无发布记录补完成/转失败 |
| 采集中旧目录照常下载 | 下载只读最新 `publications` 行，与采集/暂存事务隔离 |
| 页面显示进度、错误、可续采/放弃 | 后台任务 + 2 秒自动刷新；进度条、错误码、继续采集/重试发布/放弃本轮 |

## 测试

```bash
python3 -m pytest tests -q        # 或 docker compose exec catalog-web python -m pytest tests -q
```

覆盖：全量→同日增量、按 identifier 更新、撤回永久保留、缺席不删除、分页进度重启续采、
`badResumptionToken` 重采且水位不动、生成/落库失败保留旧目录、发布前后退出后的
“水位 ↔ 文件”一致性、目录哈希与记录数，以及放弃隔离（跨截止日窗口放弃后不泄漏、
发布失败轮放弃、放弃/继续仲裁、迟到处理拒写、重启后放弃有效、暂存合并语义）。

## 文件结构

```
app/         采集网页（main.py 路由、harvester.py 编排、oai.py 解析、catalog.py 发布、db.py SQLite）
source/      本地演示 OAI-PMH 源（同样 FastAPI + SQLite）
tests/       端到端行为测试（真实 ASGI 源，无外部依赖）
scripts/     demo_flow.py：无浏览器跑通演示（容器内 python -m scripts.demo_flow）
Dockerfile / docker-compose.yml
```

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OAI_ENDPOINT` | `http://oai-source:8001/oai` | 固定 OAI-PMH 地址 |
| `CATALOG_DATA_DIR` | `/srv/data`（compose） | SQLite 与已发布目录 |
| `MAX_PAGES_PER_CALL` | compose 默认 `1` | 单次后台采集页数上限，演示多次续采与放弃（空 = 不限） |
| `PUBLISH_FAILURE_MODE` | `none` | 首次启动时预置一次性发布故障：`generate_error` / `commit_error` / `crash_before_commit` / `crash_after_publish` |
| `SOURCE_DB_PATH` | `/srv/data/source.db` | 演示源 SQLite（`oai-source` 服务使用） |

演示源数据在 `source-data` 卷；采集端数据在 `catalog-data` 卷。

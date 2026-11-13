"""采集编排：首次全量，此后按水位增量；分页进度落库，可续采；放弃彻底隔离。

规则
----
1. 首次（无水位）：ListRecords 不带 from，window_from=NULL。
2. 此后：window_from = 上次成功发布的水位（含当天），window_until 由页面选择
   （默认今天）。from/until 都是含当天，因此同日多次变更不会漏采。
3. 每页成功后立即 UPSERT 到该轮专属暂存表 ``run_records``，并把下一页 token
   写入 runs 表；进程重启/断流后从 token 续采。重复页只产生幂等 UPSERT。
4. badResumptionToken：不推进水位、清空本轮暂存，从原窗口第一页重采。
5. 采集只写暂存，绝不触碰已发布快照 ``records``；发布时才以"最后成功发布的
   records + 本轮暂存"生成目录，并在单个事务里提交。只有发布登记成功才产生
   新水位。放弃轮次的暂存连同状态一起清空，其增改删不可能进入后续任何目录。
6. 每个写入口都带状态条件：轮次被放弃后，迟到的页处理或发布动作直接被
   RunAbandoned 拒绝，不会写入或发布任何内容。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import asyncio
import random

from .db import Database, Run, RunAbandoned, utcnow
from .oai import OAIFatalError, OAIError, OAIHttpClient, Page


class HarvestBlocked(Exception):
    pass


@dataclass
class HarvestOutcome:
    run: Run
    published: bool = False
    publication: dict | None = None
    message: str | None = None
    abandoned: bool = False


@dataclass
class Publisher:
    """把"生成 + 原子发布"包成可替换的发布动作；失败抛异常，旧目录不动。"""

    db: Database
    output_dir: str
    fault: str = "none"
    exit_process: object = None

    def publish(self, run: Run, records: list, watermark: str) -> dict:
        from .catalog import write_publication
        import os

        return write_publication(
            self.db,
            self.output_dir,
            run.id,
            watermark,
            records,
            fault=self.fault,
            exit_process=self.exit_process or os._exit,
        )


class Harvester:
    def __init__(
        self,
        db: Database,
        endpoint: str,
        publisher: Publisher,
        *,
        max_pages_per_call: int | None = None,
        max_retries: int = 2,
        retry_delay: float = 0.4,
        page_delay: float = 0.0,
        max_token_resets: int = 3,
    ):
        self.db = db
        self.endpoint = endpoint
        self.publisher = publisher
        self.max_pages_per_call = max_pages_per_call
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.page_delay = page_delay
        self.max_token_resets = max_token_resets
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ 入口
    async def start(self, window_until: date) -> Run:
        if self.db.unresolved_run() is not None:
            raise HarvestBlocked("已有未结束的轮次，请先继续/重试或放弃。")
        watermark = self.db.watermark()
        if watermark is None:
            return self.db.create_run("full", None, window_until)
        window_from = date.fromisoformat(watermark)
        if window_until < window_from:
            raise HarvestBlocked(
                f"本轮截止日 {window_until} 早于当前水位 {window_from}；"
                "增量窗口必须含上次成功截止日，不能回退。"
            )
        return self.db.create_run("incremental", window_from, window_until)

    async def resume(self, run_id: int) -> HarvestOutcome:
        """继续未结束轮次：paused/running 续采；publish_failed/publishing 只重试发布。

        状态翻转在数据库里条件完成：与"放弃"并发时只有一个动作能赢。
        """
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None:
                raise HarvestBlocked(f"轮次 #{run_id} 不存在")
            if not run.is_unresolved:
                # 放弃成功后的迟到"继续"：不复活轮次，按放弃结果回报，绝不写入/发布。
                if run.status == "canceled":
                    return await self._abandoned_outcome(run_id)
                raise HarvestBlocked(f"轮次 #{run_id} 已结束（{run.status}），无需继续")

            if run.status in ("publish_failed", "publishing"):
                if run.status == "publish_failed":
                    if not self.db.begin_publish_retry(run_id):
                        return await self._abandoned_outcome(run_id)
                    run = self.db.get_run(run_id)
                    assert run is not None
                return await self._publish(run)

            if run.status == "paused":
                if not self.db.begin_resume(run_id):
                    return await self._abandoned_outcome(run_id)
            # running：可能是任务异常退出后由路由重新唤起，直接进入采集循环；
            # 循环首行的状态检查与每张页面的写入守卫保证不会与放弃竞争。
            run = self.db.get_run(run_id)
            assert run is not None
            return await self._harvest_loop(run_id)

    async def retry_publish(self, run_id: int) -> HarvestOutcome:
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None or run.status != "publish_failed":
                raise HarvestBlocked("该轮次当前不需要重试发布")
            if not self.db.begin_publish_retry(run_id):
                return await self._abandoned_outcome(run_id)
            run = self.db.get_run(run_id)
            assert run is not None
            return await self._publish(run)

    async def retry_publish_after_begin(self, run_id: int) -> HarvestOutcome:
        """页面已把状态置为 publishing，这里直接重试（避免再次 begin 失败）。"""
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None or run.status != "publishing":
                raise HarvestBlocked("该轮次不在发布中")
            return await self._publish(run)

    async def start_in_background(self, window_until: date, on_created=None) -> HarvestOutcome:
        """创建轮次后立即采集并发布（测试/脚本便捷入口）。"""
        run = await self.start(window_until)
        if on_created:
            on_created(run)
        return await self.resume(run.id)

    # -------------------------------------------------------------- 采集循环
    async def _harvest_loop(self, run_id: int) -> HarvestOutcome:
        run = self.db.get_run(run_id)
        assert run is not None
        start_pages = run.pages_done
        token = run.resumption_token
        pages_done = run.pages_done
        records_seen = run.records_seen

        async with OAIHttpClient(self.endpoint, timeout=30.0) as client:
            while True:
                # 放弃可能发生在两次网络请求之间；每次循环先核对状态。
                run = self.db.get_run(run_id)
                if run is None or run.status not in ("running", "paused"):
                    return await self._abandoned_outcome(run_id)

                if (
                    self.max_pages_per_call is not None
                    and (pages_done - start_pages) >= self.max_pages_per_call
                ):
                    self.db.pause_run(run_id, "chunk_limit", "单次调用页数上限，进度已保存。")
                    run = self.db.get_run(run_id)
                    assert run is not None
                    return HarvestOutcome(run=run, message="progress saved (chunk limit)")

                try:
                    page = await self._fetch_page(client, run, token)
                except OAIError as exc:
                    if exc.code == "badResumptionToken":
                        if run.token_resets >= self.max_token_resets:
                            self.db.pause_run(
                                run_id,
                                "badResumptionToken",
                                f"分页凭据连续失效 {run.token_resets} 次，已停止以免循环重采。",
                            )
                            return await self._paused_outcome(run_id, "badResumptionToken")
                        note = (
                            f"分页凭据失效（badResumptionToken），已从原窗口第一页重采"
                            f"（第 {run.token_resets + 1} 次重置）；水位保持不变。"
                        )
                        if not self.db.reset_for_relist(run_id, note):
                            return await self._abandoned_outcome(run_id)
                        token = None
                        pages_done = 0
                        records_seen = 0
                        start_pages = 0
                        run = self.db.get_run(run_id)
                        assert run is not None
                        continue
                    self.db.pause_run(run_id, exc.code, exc.message or "OAI 错误")
                    return await self._paused_outcome(run_id, exc.code)
                except Exception as exc:
                    self.db.pause_run(run_id, "interrupted", f"{type(exc).__name__}: {exc}")
                    return await self._paused_outcome(run_id, "interrupted")

                for rec in page.records:
                    try:
                        self.db.upsert_run_record(
                            run_id,
                            {
                                "identifier": rec.identifier,
                                "datestamp": rec.datestamp,
                                "deleted": rec.deleted,
                                "metadata": rec.metadata,
                                "metadata_xml": rec.metadata_xml,
                                "sets": rec.sets,
                            },
                        )
                    except RunAbandoned:
                        # 放弃与网络迟到的页处理竞争：本条及后续一律不写。
                        return await self._abandoned_outcome(run_id)

                pages_done += 1
                records_seen += len(page.records)
                token = page.token
                if not self.db.update_progress(
                    run_id,
                    token=token,
                    pages_done=pages_done,
                    records_seen=records_seen,
                    complete_list_size=page.complete_list_size,
                    cursor=page.cursor,
                ):
                    # 轮次在页处理期间被放弃：进度写不进去，立即停止。
                    return await self._abandoned_outcome(run_id)

                if not token:
                    break
                if self.page_delay > 0:
                    await asyncio.sleep(self.page_delay)

        if not self.db.complete_harvest(run_id):
            return await self._abandoned_outcome(run_id)
        run = self.db.get_run(run_id)
        assert run is not None
        return await self._publish(run)

    async def _paused_outcome(self, run_id: int, code: str) -> HarvestOutcome:
        run = self.db.get_run(run_id)
        assert run is not None
        return HarvestOutcome(run=run, message=f"paused: {code}")

    async def _abandoned_outcome(self, run_id: int) -> HarvestOutcome:
        """放弃成功后的迟到处理统一出口：不写库、不发布，原样回报 canceled。"""
        run = self.db.get_run(run_id)
        if run is None or run.status != "canceled":
            # 极端情况下状态已被其他路径终结；按当前状态回报即可。
            run = run or Run(
                id=run_id, mode="unknown", window_from=None, window_until="",
                status="canceled", resumption_token=None, pages_done=0, records_seen=0,
                complete_list_size=None, cursor=None, token_resets=0, error_code=None,
                error_message=None, notice=None, started_at=utcnow(), updated_at=utcnow(),
                finished_at=utcnow(),
            )
        return HarvestOutcome(run=run, message="run was canceled", abandoned=True)

    async def _fetch_page(self, client: OAIHttpClient, run: Run, token: str | None) -> Page:
        window_from = date.fromisoformat(run.window_from) if run.window_from else None
        window_until = date.fromisoformat(run.window_until) if run.window_until else None
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                return await client.list_records(from_=window_from, until=window_until, token=token)
            except OAIError:
                raise
            except Exception as exc:  # 网络、超时、XML 损坏等：重试后交给上层暂停
                last_error = exc
            if attempt < self.max_retries:
                await asyncio.sleep(self.retry_delay * (2 ** attempt) + random.uniform(0, 0.2))
        assert last_error is not None
        raise last_error

    # ------------------------------------------------------------------ 发布
    async def _publish(self, run: Run) -> HarvestOutcome:
        # 发布前再核对一次：放弃成功后的迟到发布直接拒绝。
        fresh = self.db.get_run(run.id)
        if fresh is None or fresh.status != "publishing":
            return await self._abandoned_outcome(run.id)
        run = fresh
        watermark = run.window_until  # 只有发布登记成功，这个值才会成为新水位

        # 目录 = 最后成功发布的 records 快照 + 本轮暂存的增改删；
        # 未返回的记录与既有删除标记原样保留，绝不以缺席推断删除。
        merged = self.db.merged_active_records(run.id)
        try:
            descriptor = await asyncio.to_thread(self.publisher.publish, run, merged, watermark)
        except Exception as exc:
            # 故障注入的进程退出（SystemExit/os._exit）不落 publish_failed：
            # 进程重启后由启动恢复逻辑根据 publications 判断，保证水位与文件成对。
            # 若发布期间轮次已被放弃，条件更新不命中，按放弃回报（孤儿文件由
            # 下次成功发布/启动清理回收）。
            if not self.db.mark_publish_failed(run.id, f"{type(exc).__name__}: {exc}"):
                return await self._abandoned_outcome(run.id)
            run2 = self.db.get_run(run.id)
            assert run2 is not None
            return HarvestOutcome(run=run2, published=False, message=str(exc))

        # 单事务：暂存覆盖 records + publications 落库 + 轮次完成。
        # 条件 status='publishing' 保证放弃成功后这里整体回滚，旧目录与水位不变。
        try:
            self.db.commit_run_publication(
                run.id,
                watermark=watermark,
                file_path=descriptor["path"],
                file_name=descriptor["name"],
                record_count=descriptor["record_count"],
                size_bytes=descriptor["size_bytes"],
                sha256=descriptor["sha256"],
            )
        except RunAbandoned:
            return await self._abandoned_outcome(run.id)

        from .catalog import cleanup_orphans

        cleanup_orphans(
            self.publisher.output_dir,
            keep_paths={row["file_path"] for row in self.db.query("SELECT file_path FROM publications")},
        )
        run2 = self.db.get_run(run.id)
        assert run2 is not None
        return HarvestOutcome(run=run2, published=True, publication=descriptor)

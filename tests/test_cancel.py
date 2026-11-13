"""放弃流程的行为测试：已放弃轮次的增改删不得进入后续目录。

覆盖报告中的关键场景：
* 13 日成功 → 15 日窗口采到改名/撤回后放弃 → 再采 14 日，目录不混入 15 日变更。
* 发布失败的轮次放弃后重试发布被拒绝，其暂存不进目录。
* 放弃与继续同时提交只接纳一种；放弃成功后迟到处理不写入、不发布。
* 重启后放弃仍然有效（暂存被清理，状态保持 canceled）。
* 新一轮只在最后成功发布状态上叠加本轮暂存；未返回记录与已发布删除标记保留。
"""
from __future__ import annotations

import asyncio
from datetime import date
import os
import sys

import httpx
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import Database, RunAbandoned
from app.harvester import Harvester, Publisher
from app import oai as oai_mod
from source.app import create_source_app


@pytest.fixture()
def env(tmp_path):
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)
    orig_enter = oai_mod.OAIHttpClient.__aenter__

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    oai_mod.OAIHttpClient.__aenter__ = patched_enter
    try:
        yield tmp_path, src_app, transport
    finally:
        oai_mod.OAIHttpClient.__aenter__ = orig_enter


def make(tmp_path, *, max_pages=None, fault="none", exit_process=None):
    db = Database(str(tmp_path / "catalog.db"))
    publisher = Publisher(
        db,
        str(tmp_path / "published"),
        fault=fault,
        exit_process=exit_process or (lambda code: (_ for _ in ()).throw(SystemExit(code))),
    )
    h = Harvester(db, "http://source/oai", publisher, max_pages_per_call=max_pages, max_retries=0)
    return db, h


def run(coro):
    return asyncio.run(coro)


def source_db(app):
    return app.state.store


def catalog_xml(db) -> str:
    return open(db.latest_publication()["file_path"], encoding="utf-8").read()


def test_cross_deadline_abandoned_window_never_leaks_into_later_catalog(env):
    """报告主场景：13 日成功 → 15 日窗口（改名+撤回）放弃 → 14 日窗口发布，不含 15 日变更。"""
    tmp_path, src_app, _ = env
    store = source_db(src_app)
    db, h = make(tmp_path)

    # 1) 全量采至 13 日并成功发布
    store.redate_all("2026-11-13")
    first = run(h.start_in_background(date(2026, 11, 13)))
    assert first.run.status == "completed"
    assert db.watermark() == "2026-11-13"
    xml = catalog_xml(db)
    assert "oai:demo:lib:001" in xml and "oai:demo:lib:002" in xml

    # 2) 15 日窗口：001 改名、002 撤回。用分页受限的 harvester 复现“暂停后放弃”。
    store.update_item("oai:demo:lib:001", title="15日改名", datestamp="2026-11-15")
    store.delete_item("oai:demo:lib:002", datestamp="2026-11-15")

    db2, h2 = make(tmp_path, max_pages=1)
    r15 = run(h2.start(date(2026, 11, 15)))
    paused = run(h2.resume(r15.id))
    assert paused.run.status == "paused"
    # 第一页已暂存两条（源按 datestamp 升序，15 日记录在末页）；
    # 无论暂存了什么，已发布目录都不受影响
    assert db2.run_record_count(r15.id) == 2
    assert catalog_xml(db2).find("15日改名") < 0

    # 3) 放弃 15 日轮次：暂存整批作废
    assert db2.cancel_run(r15.id) is True
    assert db2.run_record_count(r15.id) == 0
    assert db2.get_run(r15.id).status == "canceled"
    # 已发布快照与水位不变
    assert db2.watermark() == "2026-11-13"
    assert "15日改名" not in catalog_xml(db2)
    assert "oai:demo:lib:002" in catalog_xml(db2)

    # 4) 再开 14 日窗口并发布：15 日变更不得混入
    db3, h3 = make(tmp_path)
    out14 = run(h3.start_in_background(date(2026, 11, 14)))
    assert out14.published
    xml14 = catalog_xml(db3)
    assert "15日改名" not in xml14
    assert "oai:demo:lib:002" in xml14  # 15 日的撤回也不得生效
    assert db3.watermark() == "2026-11-14"
    assert db3.record_stats() == (5, 0)

    # 5) 最后再正经采到 15 日：改名与撤回这才生效，tombstone 永久保留
    out15b = run(h3.start_in_background(date(2026, 11, 15)))
    assert out15b.published
    xml15 = catalog_xml(db3)
    assert "15日改名" in xml15
    assert "oai:demo:lib:002" not in xml15
    assert db3.record_stats() == (4, 1)


def test_abandoned_run_cannot_be_resumed_or_published(env):
    """放弃后继续/重试发布都被拒绝，迟到处理不写暂存、不产生发布。"""
    tmp_path, src_app, _ = env
    store = source_db(src_app)
    db, h = make(tmp_path, max_pages=1)
    run(h.start_in_background(date(2026, 11, 13)))
    run_id = db.unresolved_run().id

    assert db.cancel_run(run_id) is True
    # 重复放弃不生效（轮次已终结）
    assert db.cancel_run(run_id) is False
    # 状态翻转类动作全部失败
    assert db.begin_resume(run_id) is False
    assert db.begin_publish_retry(run_id) is False
    assert db.complete_harvest(run_id) is False

    # harvester 层：resume 不复活已放弃轮次，直接回报 abandoned
    outcome = run(h.resume(run_id))
    assert outcome.abandoned is True
    assert outcome.run.status == "canceled"

    # 迟到的页记录写不进暂存
    with pytest.raises(RunAbandoned):
        db.upsert_run_record(
            run_id,
            {"identifier": "oai:demo:lib:001", "datestamp": "2026-11-15",
             "deleted": False, "metadata": {"title": ["迟到写入"]},
             "metadata_xml": None, "sets": []},
        )
    assert db.run_record_count(run_id) == 0
    assert db.watermark() is None  # 从未成功发布过，水位仍为空
    assert db.latest_publication() is None


def test_publish_failed_run_abandonment_discards_changes(env):
    """发布失败的轮次一旦放弃，其增改删不得进入后续目录，重试发布被拒。"""
    tmp_path, src_app, _ = env
    store = source_db(src_app)
    db, h = make(tmp_path)
    run(h.start_in_background(date(2026, 11, 13)))
    old_pub = dict(db.latest_publication())

    store.update_item("oai:demo:lib:001", title="发布失败轮改名")
    db.set_meta("publish_fault", "generate_error")
    out = run(h.start_in_background(date(2026, 11, 13)))
    failed_id = out.run.id
    assert out.run.status == "publish_failed"
    assert db.run_record_count(failed_id) >= 1  # 暂存还在
    assert db.latest_publication()["id"] == old_pub["id"]

    # 放弃该轮：暂存清空，旧目录不变
    assert db.cancel_run(failed_id) is True
    assert db.run_record_count(failed_id) == 0
    assert db.begin_publish_retry(failed_id) is False
    assert db.latest_publication()["id"] == old_pub["id"]
    assert "发布失败轮改名" not in catalog_xml(db)

    # 新一轮（无故障）。先把源端改名回退，证明新目录只来自“最后成功发布快照 +
    # 本轮实际采得的变更”，被放弃轮次的暂存没有任何残留
    store.update_item("oai:demo:lib:001", title="图书馆学概论")
    db2, h2 = make(tmp_path)
    out2 = run(h2.start_in_background(date(2026, 11, 13)))
    assert out2.published
    assert "发布失败轮改名" not in catalog_xml(db2)


def test_cancel_and_resume_arbitration_only_one_wins(env):
    """放弃与继续同时提交：DB 条件更新仲裁，只接纳一种。"""
    tmp_path, src_app, _ = env
    db, h = make(tmp_path, max_pages=1)
    run(h.start_in_background(date(2026, 11, 13)))
    run_id = db.unresolved_run().id
    assert db.get_run(run_id).status == "paused"

    # 顺序一：放弃先赢，继续必须失败
    assert db.cancel_run(run_id) is True
    assert db.begin_resume(run_id) is False

    # 顺序二：另起一轮，继续先赢进入 running；running 轮允许放弃（采集循环的
    # 写库守卫会让迟到处理停止），放弃后该轮同样无法再继续
    r2 = run(h.start(date(2026, 11, 13)))
    paused = run(h.resume(r2.id))
    assert paused.run.status == "paused"
    assert db.begin_resume(r2.id) is True
    assert db.get_run(r2.id).status == "running"
    assert db.cancel_run(r2.id) is True
    assert db.begin_resume(r2.id) is False
    late = run(h.resume(r2.id))  # 迟到的继续不复活轮次
    assert late.abandoned is True and late.run.status == "canceled"
    assert db.run_record_count(r2.id) == 0

    # 顺序三：再一轮在 paused 状态，放弃赢则继续失败，且暂存被清空
    r3 = run(h.start(date(2026, 11, 13)))
    p3 = run(h.resume(r3.id))
    assert p3.run.status == "paused"
    assert db.run_record_count(r3.id) > 0
    assert db.cancel_run(r3.id) is True
    assert db.run_record_count(r3.id) == 0
    assert db.begin_resume(r3.id) is False
    assert db.get_run(r3.id).status == "canceled"


def test_late_page_after_cancel_does_not_write_or_publish(env):
    """放弃成功后，采集循环里迟到的页处理直接终止，不写暂存也不发布。"""
    tmp_path, src_app, _ = env
    db, h = make(tmp_path, max_pages=1)
    run(h.start_in_background(date(2026, 11, 13)))
    run_id = db.unresolved_run().id
    db.cancel_run(run_id)

    # resume 应回报 abandoned，而不是抛异常或继续采集
    outcome = run(h.resume(run_id))
    assert outcome.abandoned is True
    assert outcome.run.status == "canceled"
    assert db.run_record_count(run_id) == 0
    assert db.latest_publication() is None


def test_abandonment_survives_restart(env):
    """重启后放弃仍然有效：状态保持 canceled，暂存为空，无法继续/发布。"""
    tmp_path, src_app, _ = env
    db, h = make(tmp_path, max_pages=1)
    run(h.start_in_background(date(2026, 11, 13)))
    run_id = db.unresolved_run().id
    # 手动留一条暂存，模拟放弃流程之外的残留
    db.upsert_run_record(
        run_id,
        {"identifier": "oai:demo:lib:001", "datestamp": "2026-11-13",
         "deleted": False, "metadata": {}, "metadata_xml": None, "sets": []},
    )
    db.cancel_run(run_id)
    db.close()

    db2 = Database(str(tmp_path / "catalog.db"))
    db2.recover_after_restart()
    assert db2.get_run(run_id).status == "canceled"
    assert db2.run_record_count(run_id) == 0
    assert db2.begin_resume(run_id) is False
    assert db2.unresolved_run() is None


def test_new_run_builds_on_last_published_snapshot(env):
    """新一轮只在最后成功发布状态上叠加本轮暂存：未返回记录与既有 tombstone 保留。"""
    tmp_path, src_app, _ = env
    store = source_db(src_app)
    db, h = make(tmp_path)

    run(h.start_in_background(date(2026, 11, 13)))
    store.delete_item("oai:demo:lib:005")
    run(h.start_in_background(date(2026, 11, 13)))
    assert db.record_stats() == (4, 1)

    # 只改 001；005 不在本轮返回中，tombstone 必须保留；其他记录原样保留
    store.update_item("oai:demo:lib:001", title="只改001")
    out = run(h.start_in_background(date(2026, 11, 13)))
    assert out.published
    xml = catalog_xml(db)
    assert "只改001" in xml
    assert "oai:demo:lib:005" not in xml
    assert db.one("SELECT deleted FROM records WHERE identifier='oai:demo:lib:005'")["deleted"] == 1
    assert db.record_stats() == (4, 1)


def test_bad_token_relist_clears_staging_and_keeps_window(env):
    """凭据失效重采：清空本轮暂存从第一页重来，窗口/水位不变。"""
    tmp_path, src_app, _ = env
    db, h = make(tmp_path, max_pages=1)
    run(h.start_in_background(date(2026, 11, 13)))
    r = db.unresolved_run()
    assert r.status == "paused" and r.pages_done == 1
    assert db.run_record_count(r.id) == 2  # 每页 2 条
    src_app.state.store.invalidate_all_tokens()

    out = run(h.resume(r.id))
    assert db.get_run(r.id).token_resets >= 1
    # 重采开始时暂存被清空，之后逐页重新累积
    assert db.get_run(r.id).window_from is None
    while db.unresolved_run() is not None and db.unresolved_run().status == "paused":
        out = run(h.resume(r.id))
    assert out.run.status == "completed"
    assert db.watermark() == "2026-11-13"
    assert db.record_stats() == (5, 0)

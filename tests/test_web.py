"""网页端行为测试：后台采集、进度/错误显示、下载与水位成对。

用真实 FastAPI 路由（TestClient）+ ASGI 直连的演示源，不依赖网络。
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import oai as oai_mod
from source.app import create_source_app


@pytest.fixture()
def web(tmp_path, monkeypatch):
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    monkeypatch.setattr(oai_mod.OAIHttpClient, "__aenter__", patched_enter)
    monkeypatch.setenv("CATALOG_DATA_DIR", str(tmp_path / "web"))
    monkeypatch.setenv("OAI_ENDPOINT", "http://source/oai")
    monkeypatch.delenv("PUBLISH_FAILURE_MODE", raising=False)
    monkeypatch.delenv("MAX_PAGES_PER_CALL", raising=False)

    import importlib

    import app.main as main_mod

    importlib.reload(main_mod)
    app = main_mod.build_app()
    with TestClient(app) as client:
        yield client, app, src_app


FINAL = {"completed", "paused", "publish_failed", "canceled"}


def _wait(client: TestClient, timeout: float = 30.0) -> str:
    """等待最近一轮进入终态/暂停（直接看 DB 判定比页面轮询可靠）。"""
    db = client.app.state.db
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = db.recent_runs(1)[0] if db.recent_runs(1) else None
        if run is not None and run.status in FINAL:
            break
        time.sleep(0.1)
    return client.get("/").text


def test_home_renders(web):
    client, app, _ = web
    r = client.get("/")
    assert r.status_code == 200
    assert "本轮截止日" in r.text


def test_background_harvest_publishes_and_download_pairs(web):
    client, app, src_app = web
    r = client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    assert r.status_code == 200
    html = _wait(client)
    assert "已完成" in html

    db = app.state.db
    pub = db.latest_publication()
    dl = client.get("/catalog/download")
    assert dl.status_code == 200
    assert dl.headers["x-catalog-watermark"] == pub["watermark"]
    assert int(dl.headers["x-catalog-run"]) == pub["run_id"]
    assert f'watermark="{pub["watermark"]}"' in dl.text
    assert dl.text.count("<record ") == pub["record_count"]
    assert "oai:demo:lib:001" in dl.text

    # 新一轮：同日更新 + 撤回
    src_app.state.store.update_item("oai:demo:lib:001", title="网页测试改名")
    src_app.state.store.delete_item("oai:demo:lib/003".replace("/", ":"))
    client.post("/harvest/start", data={"until": "2026-11-13"})
    html = _wait(client)
    assert "已完成" in html
    pub2 = db.latest_publication()
    assert pub2["id"] != pub["id"]
    dl2 = client.get("/catalog/download")
    assert "网页测试改名" in dl2.text
    assert "oai:demo:lib:003" not in dl2.text
    assert db.watermark() == "2026-11-13"


def test_download_available_during_harvest_and_progress_visible(web, monkeypatch):
    client, app, src_app = web
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait(client)
    first = app.state.db.latest_publication()

    # 让源端第一页响应变慢，采集中时下载仍可用
    from source.app import SourceStore

    orig = SourceStore._all

    def slow(self, sql, *params):
        if "FROM items" in sql:
            time.sleep(0.4)
        return orig(self, sql, *params)

    monkeypatch.setattr(SourceStore, "_all", slow)
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    r = client.get("/")
    # 采集中：页面有进度，旧目录可下载
    assert "采集中" in r.text or "发布中" in r.text or "已完成" in r.text
    dl = client.get("/catalog/download")
    assert dl.status_code == 200
    assert int(dl.headers["x-catalog-run"]) == first["run_id"]
    _wait(client)


def test_publish_failure_shows_retry_and_keeps_old_catalog(web):
    client, app, src_app = web
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait(client)
    db = app.state.db
    old = db.latest_publication()
    old_xml = client.get("/catalog/download").text

    src_app.state.store.update_item("oai:demo:lib:001", title="发布失败改名")
    db.set_meta("publish_fault", "generate_error")
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    deadline = time.time() + 30
    while time.time() < deadline and db.recent_runs(1)[0].status != "publish_failed":
        time.sleep(0.1)
    html = _wait(client)
    assert "发布失败" in html
    assert "/retry-publish" in html
    assert db.latest_publication()["id"] == old["id"]
    assert client.get("/catalog/download").text == old_xml
    assert "发布失败改名" not in old_xml

    run = db.recent_runs(1)[0]
    assert run.status == "publish_failed"
    client.post(f"/harvest/{run.id}/retry-publish", follow_redirects=True)
    deadline = time.time() + 30
    while time.time() < deadline and db.get_run(run.id).status != "completed":
        time.sleep(0.1)
    assert db.get_run(run.id).status == "completed"
    assert "发布失败改名" in client.get("/catalog/download").text


def test_paused_run_has_resume_button(web):
    client, app, src_app = web
    # 用故障注入让采集立刻中断：源端返回 500？这里直接暂停 run
    db = app.state.db
    run = db.create_run("full", None, date(2026, 11, 13))
    db.pause_run(run.id, "interrupted", "测试中断")
    html = client.get("/").text
    assert "已暂停·可续采" in html
    assert f"/harvest/{run.id}/resume" in html
    client.post(f"/harvest/{run.id}/resume", follow_redirects=True)
    deadline = time.time() + 30
    while time.time() < deadline and app.state.db.get_run(run.id).status != "completed":
        time.sleep(0.1)
    assert app.state.db.get_run(run.id).status == "completed"
    assert "已完成" in client.get("/").text


def test_cancel_paused_run_then_new_run_is_clean(web):
    """页面放弃暂停轮次后，暂存清空；再开一轮发布，被放弃的变更不进目录。"""
    client, app, src_app = web
    db = app.state.db
    # 先有一份成功发布
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait(client)
    assert db.watermark() == "2026-11-13"

    # 制造一个暂停轮次：直接插 run 并暂停
    src_app.state.store.update_item(
        "oai:demo:lib:001", title="应被放弃的改名", datestamp="2026-11-15"
    )
    run = db.create_run("incremental", date(2026, 11, 13), date(2026, 11, 15))
    db.upsert_run_record(
        run.id,
        {"identifier": "oai:demo:lib:001", "datestamp": "2026-11-15", "deleted": 0,
         "metadata": {"title": ["应被放弃的改名"]}, "metadata_xml": None, "sets": []},
    )
    db.pause_run(run.id, "interrupted", "测试中断")
    assert db.run_record_count(run.id) == 1
    html = client.get("/").text
    assert f"/harvest/{run.id}/cancel" in html

    r = client.post(f"/harvest/{run.id}/cancel", follow_redirects=True)
    assert r.status_code == 200
    assert db.get_run(run.id).status == "canceled"
    assert db.run_record_count(run.id) == 0
    old_xml = client.get("/catalog/download").text
    assert "应被放弃的改名" not in old_xml

    # 源端把改名回退后再采一轮：目录依然干净，水位正常推进
    src_app.state.store.update_item("oai:demo:lib:001", title="图书馆学概论", datestamp="2026-11-14")
    client.post("/harvest/start", data={"until": "2026-11-14"}, follow_redirects=True)
    _wait(client)
    assert db.watermark() == "2026-11-14"
    assert "应被放弃的改名" not in client.get("/catalog/download").text


def test_cancel_while_running_stops_late_work(web, monkeypatch):
    """采集中放弃：正在运行的任务停止写入，已发布目录与水位不变。"""
    client, app, src_app = web
    db = app.state.db
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait(client)
    pubs_before = len(db.publications(50))

    # 拖慢源端响应，保证第二轮采集进行中可以插入放弃
    from source.app import SourceStore

    orig = SourceStore._all

    def slow(self, sql, *params):
        if "FROM items" in sql:
            time.sleep(0.5)
        return orig(self, sql, *params)

    monkeypatch.setattr(SourceStore, "_all", slow)
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    time.sleep(0.3)
    active = db.unresolved_run()
    assert active is not None and active.status in ("running", "publishing")
    r = client.post(f"/harvest/{active.id}/cancel", follow_redirects=True)
    assert r.status_code == 200

    deadline = time.time() + 30
    while time.time() < deadline:
        run = db.get_run(active.id)
        if run.status == "canceled":
            break
        time.sleep(0.1)
    assert db.get_run(active.id).status == "canceled"
    assert db.run_record_count(active.id) == 0
    assert len(db.publications(50)) == pubs_before  # 没有产生新发布


def test_cancel_and_resume_posts_only_one_takes_effect(web):
    """放弃与继续两个 POST 同时到达：只有一个生效，另一个收到错误提示。"""
    client, app, src_app = web
    db = app.state.db
    run = db.create_run("full", None, date(2026, 11, 13))
    db.pause_run(run.id, "interrupted", "测试中断")

    r_cancel = client.post(f"/harvest/{run.id}/cancel", follow_redirects=False)
    r_resume = client.post(f"/harvest/{run.id}/resume", follow_redirects=False)
    statuses = {r_cancel.status_code, r_resume.status_code}
    assert statuses == {303}
    final = db.get_run(run.id)
    assert final.status in ("canceled", "running")
    if final.status == "canceled":
        # 放弃赢：继续的重定向带 error，且轮次不会被后台任务复活
        assert "error" in r_resume.headers["location"]
        time.sleep(0.5)
        assert db.get_run(run.id).status == "canceled"
    else:
        # 继续赢：放弃的重定向带 error，任务正常采完
        assert "error" in r_cancel.headers["location"]
        deadline = time.time() + 30
        while time.time() < deadline and db.get_run(run.id).status != "completed":
            time.sleep(0.1)
        assert db.get_run(run.id).status == "completed"


def test_canceled_publish_failed_run_cannot_retry(web):
    """发布失败轮次被放弃后，重试发布被拒，旧目录不变。"""
    client, app, src_app = web
    db = app.state.db
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait(client)
    old = db.latest_publication()

    src_app.state.store.update_item("oai:demo:lib:001", title="失败轮改名")
    db.set_meta("publish_fault", "generate_error")
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    deadline = time.time() + 30
    while time.time() < deadline and db.recent_runs(1)[0].status != "publish_failed":
        time.sleep(0.1)
    run = db.recent_runs(1)[0]
    assert run.status == "publish_failed"

    r = client.post(f"/harvest/{run.id}/cancel", follow_redirects=True)
    assert r.status_code == 200
    assert db.get_run(run.id).status == "canceled"
    r2 = client.post(f"/harvest/{run.id}/retry-publish", follow_redirects=False)
    assert "error" in r2.headers["location"]
    assert db.latest_publication()["id"] == old["id"]
    assert db.run_record_count(run.id) == 0


def test_running_run_without_live_task_can_be_resumed(web):
    """任务异常退出后留下的 running 轮次：路由允许重新唤起并正常采完。"""
    client, app, src_app = web
    db = app.state.db
    run = db.create_run("full", None, date(2026, 11, 13))
    assert db.get_run(run.id).status == "running"

    r = client.post(f"/harvest/{run.id}/resume", follow_redirects=False)
    assert r.status_code == 303 and "error" not in r.headers["location"]
    deadline = time.time() + 30
    while time.time() < deadline and db.get_run(run.id).status != "completed":
        time.sleep(0.1)
    assert db.get_run(run.id).status == "completed"
    assert db.watermark() == "2026-11-13"
    assert db.run_record_count(run.id) == 5

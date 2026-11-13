"""命令行演示脚本（可选）：无需浏览器即可跑通报修的完整场景。

用法（在容器内）：
    python -m scripts.demo_flow

全程只调用两个服务的 HTTP 接口，等价于在页面上点击，覆盖：
1. 首次全量采至 13 日并发布（分页续采）；
2. 15 日窗口采到改名/撤回，暂停后放弃 —— 暂存整批作废；
3. 放弃与继续并发提交只接纳一种；
4. 再采 14 日并发布 —— 目录不混入 15 日变更（报告主缺陷）；
5. 正式采到 15 日 —— 改名/撤回这才生效，删除标记永久保留；
6. 分页凭据失效重采（原窗口第一页，水位不动）；
7. 发布失败轮放弃后重试被拒，旧目录与水位不变。
"""
from __future__ import annotations

import os
import sys
import time

import httpx

WEB = os.environ.get("DEMO_WEB", "http://catalog-web:8000")
SOURCE = os.environ.get("DEMO_SOURCE", "http://oai-source:8001")
TERMINAL = {"completed", "paused", "publish_failed", "canceled"}


def check(cond: bool, msg: str) -> None:
    if not cond:
        print(f"断言失败：{msg}", file=sys.stderr)
        sys.exit(1)
    print(f"   OK：{msg}")


def wait_idle(client: httpx.Client, timeout: float = 60.0) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = client.get("/status").json()["latest_run"]["status"]
        if st in TERMINAL:
            return st
        time.sleep(0.3)
    raise TimeoutError("轮次未进入终态/暂停态")


def latest_run_id(client: httpx.Client) -> int:
    data = client.get("/status").json()
    if not data["latest_run"]:
        raise RuntimeError("服务端尚无轮次")
    return int(data["latest_run"]["id"])


def start_and_drain(client: httpx.Client, until: str) -> str:
    """开始一轮并反复“继续采集”直到结束（compose 下每次调用只采 1 页）。"""
    client.post("/harvest/start", data={"until": until})
    rid = latest_run_id(client)
    st = wait_idle(client)
    guard = 0
    while st == "paused":
        client.post(f"/harvest/{rid}/resume")
        st = wait_idle(client)
        guard += 1
        if guard > 20:
            raise RuntimeError("续采次数异常")
    return st


def download(client: httpx.Client):
    r = client.get("/catalog/download")
    r.raise_for_status()
    return r.headers.get("x-catalog-watermark"), r.text


def main() -> None:
    with httpx.Client(base_url=WEB, timeout=30, follow_redirects=False) as web, \
            httpx.Client(base_url=SOURCE, timeout=30) as src:
        print("1) 源端全部改期到 13 日，首次全量（分页续采）")
        src.post("/demo/redate-all", data={"datestamp": "2026-11-13"})
        check(start_and_drain(web, "2026-11-13") == "completed", "首次全量完成")
        wm, xml = download(web)
        check(wm == "2026-11-13", "水位=2026-11-13")
        check(xml.count("<record ") == 5, "首版目录 5 条")

        print("2) 15 日窗口：001 改名、002 撤回，采两页后放弃")
        src.post("/demo/update", data={
            "identifier": "oai:demo:lib:001", "title": "15日改名", "datestamp": "2026-11-15"})
        src.post("/demo/delete", data={
            "identifier": "oai:demo:lib:002", "datestamp": "2026-11-15"})
        web.post("/harvest/start", data={"until": "2026-11-15"})
        rid15 = latest_run_id(web)
        check(wait_idle(web) == "paused", "第一页后暂停")
        web.post(f"/harvest/{rid15}/resume")
        check(wait_idle(web) == "paused", "第二页后仍暂停")
        r = web.post(f"/harvest/{rid15}/cancel")
        check(r.status_code == 303 and "message" in r.headers["location"], "放弃请求被接受")
        check(wait_idle(web) == "canceled", "轮次状态=已放弃")
        check(web.get("/status").json()["watermark"] == "2026-11-13", "放弃后水位不变")
        wm, xml = download(web)
        check("15日改名" not in xml, "已发布目录不含 15 日改名")
        check("oai:demo:lib:002" in xml, "002 仍在已发布目录")

        print("3) 放弃与继续并发 POST：只接纳一种")
        web.post("/harvest/start", data={"until": "2026-11-15"})
        ridr = latest_run_id(web)
        wait_idle(web)
        loc_cancel = web.post(f"/harvest/{ridr}/cancel").headers["location"]
        loc_resume = web.post(f"/harvest/{ridr}/resume").headers["location"]
        check(("message" in loc_cancel) ^ ("message" in loc_resume), "两个动作只有一个成功")
        st = wait_idle(web)
        if st == "running":
            # 继续胜出：后台任务会自行翻页直到终态，等它跑完
            deadline = time.time() + 60
            while time.time() < deadline:
                st = web.get("/status").json()["latest_run"]["status"]
                if st in TERMINAL:
                    break
                time.sleep(0.3)
        check(st in ("canceled", "completed"), f"仲裁后终态合法：{st}")

        print("4) 再采 14 日窗口：15 日变更不得混入")
        check(start_and_drain(web, "2026-11-14") == "completed", "14 日窗口完成")
        wm, xml = download(web)
        check(wm == "2026-11-14", "水位=2026-11-14")
        check("15日改名" not in xml, "14 日目录不含 15 日改名")
        check("oai:demo:lib:002" in xml, "14 日目录仍含 002（15 日撤回未生效）")
        check(xml.count("<record ") == 5, "14 日目录仍为 5 条")

        print("5) 正式采到 15 日：改名/撤回生效，tombstone 永久保留")
        check(start_and_drain(web, "2026-11-15") == "completed", "15 日窗口完成")
        wm, xml = download(web)
        check(wm == "2026-11-15", "水位=2026-11-15")
        check("15日改名" in xml, "15 日目录含改名")
        check("oai:demo:lib:002" not in xml, "002 已撤回，不进目录")
        check(xml.count("<record ") == 4, "15 日目录 4 条有效")

        print("6) 分页凭据失效：清空暂存回原窗口第一页重采，水位不动")
        # 把其余记录也挂到 15 日，使本轮多页、第二页必用 token
        for ident in ("oai:demo:lib:003", "oai:demo:lib:004", "oai:demo:lib:005"):
            src.post("/demo/update", data={
                "identifier": ident, "subject": "凭据重采演示", "datestamp": "2026-11-15"})
        src.post("/demo/trip-token")
        before = wm
        check(start_and_drain(web, "2026-11-15") == "completed", "凭据失效后重采完成")
        check(web.get("/status").json()["watermark"] == before, "重采前后水位一致")
        check("15日改名" in download(web)[1], "重采后内容不丢")

        print("7) 发布失败轮放弃：重试被拒，旧目录不变")
        src.post("/demo/update", data={
            "identifier": "oai:demo:lib:003", "title": "失败轮改名", "datestamp": "2026-11-15"})
        web.post("/config/fault", data={"fault": "generate_error"})
        before_pub = web.get("/status").json()["latest_publication"]["id"]
        web.post("/harvest/start", data={"until": "2026-11-15"})
        ridp = latest_run_id(web)
        st = wait_idle(web)
        guard = 0
        while st == "paused":  # 先翻完分页，才会进入发布并触发故障
            web.post(f"/harvest/{ridp}/resume")
            st = wait_idle(web)
            guard += 1
            if guard > 20:
                raise RuntimeError("续采次数异常")
        check(st == "publish_failed", "进入发布失败状态")
        check(web.post(f"/harvest/{ridp}/cancel").status_code == 303, "放弃成功")
        check(wait_idle(web) == "canceled", "状态=已放弃")
        check("error" in web.post(f"/harvest/{ridp}/retry-publish").headers["location"],
              "放弃后重试发布被拒")
        check(web.get("/status").json()["latest_publication"]["id"] == before_pub,
              "publications 未新增")
        check("失败轮改名" not in download(web)[1], "失败轮改名未进入目录")

        print("\n全部演示断言通过。")


if __name__ == "__main__":
    main()

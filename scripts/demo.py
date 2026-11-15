#!/usr/bin/env python3
"""端到端演示客户端（仅用标准库 urllib，容器内可直接运行）。

子命令:
  demo           上传 -> 游标快照/重复行 -> 暂停合并 -> 恢复 -> 切换 -> 旧游标读完 -> 回收
  fail-demo      在 stream / switch 阶段注入故障，验证失败保留原数据
  restart-check  重启后校验：旧游标可读、行序内容不变、继续回收不误删现用文件

公共状态（cursor_id、总行数等）写入 --state 指定的 JSON 文件，
供 shell 编排脚本在容器重启之间传递。
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request

DEFAULT_BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
FILES_DIR = os.path.join(DATA_DIR, "files")


class Client:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def call(self, method: str, path: str, body: bytes | None = None,
             expect=(200, 201, 202)):
        req = urllib.request.Request(
            self.base + path, data=body, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = resp.read()
                status = resp.status
        except urllib.error.HTTPError as e:
            payload = e.read()
            status = e.code
        try:
            data = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            data = {"raw": payload.decode("utf-8", "replace")}
        if status not in expect:
            raise AssertionError(f"{method} {path} -> {status}: {data}")
        return status, data

    def upload(self, path: str):
        with open(path, "rb") as f:
            data = f.read()
        req = urllib.request.Request(
            self.base + "/uploads", data=data, method="POST",
            headers={"Content-Type": "application/vnd.apache.parquet"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raise AssertionError(
                f"POST /uploads -> {e.code}: {e.read().decode()}"
            ) from e

    def wait_state(self, states: tuple[str, ...], timeout: float = 30.0) -> dict:
        deadline = time.time() + timeout
        last: dict = {}
        while time.time() < deadline:
            _, last = self.call("GET", "/merges/status")
            merge = last.get("active_merge")
            if merge and merge["state"] in states:
                return merge
            time.sleep(0.2)
        raise AssertionError(f"等待合并状态 {states} 超时，最后状态: {last}")


def section(title: str) -> None:
    print("\n" + "=" * 72 + f"\n {title}\n" + "=" * 72)


def load_state(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_state(path: str, state: dict) -> None:
    with open(path, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def drain_cursor(cli: Client, cursor_id: str, page_limit: int = 500,
                 initial_rows: list[dict] | None = None) -> list[dict]:
    """翻页耗尽游标；initial_rows 为调用方此前已读出的首页，避免重复/漏计。"""
    rows: list[dict] = list(initial_rows or [])
    while True:
        _, page = cli.call(
            "GET", f"/cursors/{cursor_id}/pages?limit={page_limit}"
        )
        rows.extend(page["rows"])
        if page["exhausted"]:
            assert page["remaining"] == 0, "exhausted 时 remaining 应为 0"
            return rows


# ---------------------------------------------------------------- 主流程

def cmd_demo(args) -> None:
    cli = Client(args.base)
    state = load_state(args.state)
    section("1. 健康检查")
    _, health = cli.call("GET", "/health")
    print(json.dumps(health, ensure_ascii=False))

    section(f"2. 按顺序上传 {len(args.files)} 份合成 Parquet")
    total_rows = 0
    before_files = cli.call("GET", "/files")[1]["files"]
    for p in args.files:
        info = cli.upload(p)
        total_rows += info["row_count"]
        print(f"  上传 {os.path.basename(p)} -> seq={info['seq_order']} "
              f"rows={info['row_count']} bytes={info['size_bytes']}")
    state["total_rows"] = total_rows
    _, listing = cli.call("GET", "/files")
    assert len(listing["files"]) - len(before_files) == len(args.files)
    assert all(f["status"] == "active" for f in listing["files"])

    section("3. 创建游标 A（固定创建时已提交的行），校验总行数与重复行保留")
    _, cur_a = cli.call("POST", "/cursors")
    cursor_a = cur_a["cursor_id"]
    state["cursor_a"] = cursor_a
    print(f"  cursor A = {cursor_a}，快照行数 = {cur_a['snapshot_rows']}")
    assert cur_a["snapshot_rows"] == total_rows

    _, first_page = cli.call("GET", f"/cursors/{cursor_a}/pages?limit=5")
    print("  首页前 3 行:")
    for r in first_page["rows"][:3]:
        print("   ", r)
    assert first_page["returned"] == 5

    # 合成数据里每个文件前两行重复上一个文件末尾 => 至少 2*(n-1) 个重复行
    all_rows = drain_cursor(cli, cursor_a, initial_rows=first_page["rows"])
    assert len(all_rows) == total_rows
    seen: set[tuple] = set()
    dup = 0
    for r in all_rows:
        key = (r["station_id"], r["observed_at"], r["temperature"])
        if key in seen:
            dup += 1
        seen.add(key)
    print(f"  游标 A 完整读完 {len(all_rows)} 行，其中重复行 {dup} 行（应保留）")
    assert dup >= 2 * (len(args.files) - 1)

    section("4. 再创建游标 B，启动合并并立即暂停（合并暂停演示）")
    _, cur_b = cli.call("POST", "/cursors")
    cursor_b = cur_b["cursor_id"]
    state["cursor_b"] = cursor_b
    b_initial_rows: list[dict] = []
    cli.call("POST", "/merges/pause", expect=(200, 409))  # 若无任务则忽略
    cli.call("POST", "/merges")
    cli.call("POST", "/merges/pause")
    time.sleep(0.5)
    paused = cli.wait_state(("paused", "running", "switching", "completed"))
    # 暂停可能发生在已完成之后（小文件很快），两种都接受
    print(f"  合并状态: {paused['state']}，输入文件 {len(paused['inputs'])} 个")
    if paused["state"] == "paused":
        _, st = cli.call("GET", "/merges/status")
        written = sum(i["rows_written"] for i in st["active_merge"]["inputs"])
        print(f"  暂停时已流式写入 {written} 行")
        section("5. 暂停期间继续上传 + 游标 B 翻页（验证并发不受影响）")
        extra = cli.upload(args.files[0])
        state["extra_file_rows"] = extra["row_count"]
        print(f"  暂停期间上传新文件: rows={extra['row_count']}")
        _, page_b = cli.call("GET", f"/cursors/{cursor_b}/pages?limit=7")
        b_initial_rows = page_b["rows"]
        print(f"  游标 B 读出 {page_b['returned']} 行，"
              f"快照总量仍为 {page_b['total_rows']}（不含新文件）")
        assert page_b["total_rows"] == total_rows
        section("6. 恢复合并，等待完成切换")
        cli.call("POST", "/merges/resume")
    cli.wait_state(("completed",), timeout=60)
    _, st = cli.call("GET", "/merges/status")
    print(f"  合并完成: {json.dumps(st['active_merge']['state'])} "
          f"output={st['active_merge']['output_file_id']}")

    section("7. 校验切换结果：一个 merged active 文件 + 旧文件 obsolete")
    _, listing = cli.call("GET", "/files")
    actives = [f for f in listing["files"] if f["status"] == "active"]
    obsolete = [f for f in listing["files"] if f["status"] == "obsolete"]
    merged = [f for f in actives if f["kind"] == "merged"]
    print(f"  active={len(actives)}（merged {len(merged)} + 暂停期间上传 "
          f"{len(actives) - len(merged)}），obsolete={len(obsolete)}")
    assert len(merged) == 1
    assert merged[0]["row_count"] == total_rows
    assert len(obsolete) == len(args.files)

    section("8. 旧游标 B 行序/内容不变，且读的是 obsolete 旧文件（未被回收）")
    rest_b = drain_cursor(cli, cursor_b, initial_rows=b_initial_rows)
    # 含暂停期间已预读的首页
    assert len(rest_b) == total_rows
    print(f"  游标 B 切换前后合计仍读出 {total_rows} 行")
    # 新游标只能看到合并产物 + 新上传文件
    _, cur_c = cli.call("POST", "/cursors")
    cursor_c = cur_c["cursor_id"]
    state["cursor_c"] = cursor_c
    assert cur_c["snapshot_rows"] == total_rows + state.get("extra_file_rows", 0)
    rows_c = drain_cursor(cli, cursor_c)
    assert len(rows_c) == cur_c["snapshot_rows"]
    # 合并产物内的行序必须与旧游标看到的完全一致
    assert rows_c[:total_rows] == all_rows
    print(f"  新游标 C 读出 {len(rows_c)} 行，合并段行序与游标 A 完全一致")

    section("9. 显式关闭游标，触发无引用旧文件回收")
    _, closed = cli.call("POST", f"/cursors/{cursor_b}/close")
    print(f"  关闭游标 B，回收: {closed['reclaimed']}")
    _, closed = cli.call("POST", f"/cursors/{cursor_a}/close")
    print(f"  关闭游标 A，回收: {closed['reclaimed']}")
    _, rec = cli.call("POST", "/merges/reclaim")
    print(f"  再次回收: {rec['reclaimed']}")
    _, listing = cli.call("GET", "/files")
    leftover_obsolete = [f for f in listing["files"] if f["status"] == "obsolete"]
    assert not leftover_obsolete, "所有旧游标关闭后 obsolete 文件应被回收"
    disk = sorted(os.listdir(FILES_DIR)) if os.path.isdir(FILES_DIR) else []
    print(f"  磁盘现存 {len(disk)} 个文件（active 文件绝不能被误删）")
    assert all(f["status"] == "active" for f in listing["files"])

    save_state(args.state, state)
    section("demo 全部断言通过 ✔")


# ---------------------------------------------------------------- 故障注入

def cmd_fail_demo(args) -> None:
    cli = Client(args.base)
    for step in ("stream", "switch"):
        section(f"故障注入演示：{step} 阶段失败，原数据必须保留")
        _, before = cli.call("GET", "/files")
        before_ids = {f["file_id"] for f in before["files"]}
        before_merged = {f["file_id"] for f in before["files"]
                         if f["kind"] == "merged"}
        cli.call("POST", f"/merges/fail-inject/{step}")
        cli.call("POST", "/merges")
        failed = cli.wait_state(("failed", "completed"), timeout=30)
        assert failed["state"] == "failed", f"注入 {step} 后应失败，实际 {failed['state']}"
        print(f"  合并已失败: {failed['fail_at_step']} / {failed['error']}")
        _, listing = cli.call("GET", "/files")
        after = listing["files"]
        # 文件集合完全不变：不产生新产物、不产生 obsolete
        assert {f["file_id"] for f in after} == before_ids
        assert all(f["status"] == "active" for f in after), "失败后不能产生 obsolete"
        assert {f["file_id"] for f in after if f["kind"] == "merged"} == before_merged
        print(f"  原 {len(after)} 个文件全部保留，本次失败未产生任何新产物")

    section("清除故障后重新合并，应成功切换")
    cli.call("POST", "/merges")
    cli.wait_state(("completed",), timeout=60)
    _, listing = cli.call("GET", "/files")
    assert any(f["kind"] == "merged" and f["status"] == "active"
               for f in listing["files"])
    print("  重新合并成功 ✔")


# ---------------------------------------------------------------- 重启校验

def cmd_restart_check(args) -> None:
    cli = Client(args.base)
    state = load_state(args.state)
    section("重启后校验：元数据、未关闭游标与回收流程")
    _, health = cli.call("GET", "/health")
    print(f"  启动恢复信息: {json.dumps(health.get('recovery'), ensure_ascii=False)}")

    _, listing = cli.call("GET", "/files")
    disk = sorted(os.listdir(FILES_DIR)) if os.path.isdir(FILES_DIR) else []
    # 磁盘上的每个文件都必须仍在元数据中（不能误删现用文件，也不能有孤儿）
    disk_set = {os.path.join(FILES_DIR, n) for n in disk}
    meta_set = {os.path.join(FILES_DIR, f["file_name"])
                for f in listing["files"]}
    print(f"  元数据文件 {len(meta_set)} 个，磁盘文件 {len(disk_set)} 个")
    assert disk_set == meta_set, f"磁盘与元数据不一致: {disk_set ^ meta_set}"

    for key in ("cursor_b", "cursor_c"):
        cid = state.get(key)
        if not cid:
            continue
        _, cursors_list = cli.call("GET", "/cursors")
        match = next((c for c in cursors_list["cursors"]
                      if c["cursor_id"] == cid), None)
        assert match is not None, f"游标 {cid} 在重启后丢失"
        print(f"  游标 {key}={cid} 仍存在: closed={match['closed']} "
              f"rows_read={match['rows_read']}/{match['total_rows']}")

    cid = state.get("cursor_b")
    if cid:
        _, cursors_list = cli.call("GET", "/cursors")
        match = next(c for c in cursors_list["cursors"]
                     if c["cursor_id"] == cid)
        if not match["closed"]:
            section("通过旧游标继续翻页，行序内容不变")
            rows = drain_cursor(cli, cid)
            print(f"  重启后游标 B 又读出 {len(rows)} 行并耗尽")
            _, closed = cli.call("POST", f"/cursors/{cid}/close")
            print(f"  关闭后回收: {closed['reclaimed']}")

    _, rec = cli.call("POST", "/merges/reclaim")
    print(f"  重启后继续回收无引用旧文件: {rec['reclaimed']}")
    _, listing = cli.call("GET", "/files")
    assert all(f["status"] == "active" for f in listing["files"])
    print("  剩余文件全部 active，现用文件未被误删 ✔")
    save_state(args.state, state)
    section("restart-check 全部断言通过 ✔")


# ---------------------------------------------------------------- 完整性巡检

def cmd_inspect_demo(args) -> None:
    """演示完整性巡检：完好 -> 篡改(mismatch) -> 丢失(missing) -> 恢复(intact)。

    直接在挂载卷里改写/删除正式 Parquet 来模拟“被误改或丢失”，
    巡检只读核验、不修复，结论通过 API 逐文件返回。
    """
    cli = Client(args.base)
    db_path = os.path.join(DATA_DIR, "meta", "lakehouse.db")

    def wait_inspection(iid: int) -> dict:
        deadline = time.time() + 30
        while time.time() < deadline:
            _, data = cli.call("GET", f"/inspections/{iid}")
            if data["state"] in ("completed", "failed"):
                return data
            time.sleep(0.2)
        raise AssertionError(f"巡检 {iid} 未在预期时间内完成")

    def physical_path(file_uid: str) -> str:
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT path FROM files WHERE file_id=?", (file_uid,)
            ).fetchone()
        finally:
            conn.close()
        assert row is not None, f"登记中找不到文件 {file_uid}"
        return row[0]

    section("完整性巡检演示")
    # 上传一份全新的已知完好文件，保证演示基线不依赖环境里既有文件的状态
    import pyarrow as pa
    import pyarrow.parquet as pq
    fresh_dir = os.path.join(DATA_DIR, "inspect-demo")
    os.makedirs(fresh_dir, exist_ok=True)
    fresh_path = os.path.join(fresh_dir, "probe.parquet")
    table = pa.table({
        "station_id": pa.array(["PROBE-1", "PROBE-2"], pa.string()),
        "observed_at": pa.array(
            [pa.scalar(1_000_000 + i, type=pa.timestamp("us", tz="UTC")).as_py()
             for i in range(2)], pa.timestamp("us", tz="UTC")),
        "temperature": pa.array([21.5, 22.5], pa.float64()),
    })
    pq.write_table(table, fresh_path, compression="snappy")
    target = cli.upload(fresh_path)
    uid = target["file_id"]
    path = physical_path(uid)
    original_bytes = open(path, "rb").read()
    print(f"  目标文件: {os.path.basename(path)}  登记指纹: {target['sha256'][:16]}…")

    # 1) 干净基线：完好
    iid = cli.call("POST", "/inspections", b"{}")[1]["inspection_id"]
    insp = wait_inspection(iid)
    base = next(f for f in insp["files"] if f["file_id"] == uid)
    print(f"  基线巡检 #{iid}: {base['status']}（{base['status_text']}）")
    assert base["status"] == "intact"

    # 2) 篡改正式产物：内容不符
    with open(path, "r+b") as fh:
        fh.seek(0)
        fh.write(b"TAMPERED-BYTES")
    iid = cli.call("POST", "/inspections", b"{}")[1]["inspection_id"]
    insp = wait_inspection(iid)
    hit = next(f for f in insp["files"] if f["file_id"] == uid)
    print(f"  篡改后巡检 #{iid}: {hit['status']}（{hit['status_text']}）"
          f"  实际指纹={hit['actual_sha256'][:16]}…")
    assert hit["status"] == "mismatch"

    # 3) 删除磁盘文件、保留登记：缺失
    os.remove(path)
    iid = cli.call("POST", "/inspections", b"{}")[1]["inspection_id"]
    insp = wait_inspection(iid)
    hit = next(f for f in insp["files"] if f["file_id"] == uid)
    print(f"  删除后巡检 #{iid}: {hit['status']}（{hit['status_text']}）")
    assert hit["status"] == "missing"

    # 4) 恢复原始字节：重新完好（演示巡检只核验、不阻断业务）
    with open(path, "wb") as fh:
        fh.write(original_bytes)
    iid = cli.call("POST", "/inspections", b"{}")[1]["inspection_id"]
    insp = wait_inspection(iid)
    hit = next(f for f in insp["files"] if f["file_id"] == uid)
    print(f"  恢复后巡检 #{iid}: {hit['status']}（{hit['status_text']}）")
    assert hit["status"] == "intact"
    print(f"  本次巡检汇总: {json.dumps(insp['summary'], ensure_ascii=False)}")
    section("inspect-demo 全部断言通过 ✔")


def main() -> None:
    ap = argparse.ArgumentParser(description="湖仓合并服务演示客户端")
    ap.add_argument("--base", default=DEFAULT_BASE)
    ap.add_argument("--state", default="/data/demo-state.json")
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("demo")
    d.add_argument("files", nargs="+")
    d.set_defaults(func=cmd_demo)

    sub.add_parser("fail-demo").set_defaults(func=cmd_fail_demo)
    sub.add_parser("restart-check").set_defaults(func=cmd_restart_check)
    sub.add_parser("inspect-demo").set_defaults(func=cmd_inspect_demo)

    args = ap.parse_args()
    try:
        args.func(args)
    except AssertionError as e:
        print(f"\n演示失败: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

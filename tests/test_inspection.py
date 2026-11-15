"""完整性巡检测试：指纹登记、完好/不符/缺失/未核验/合法回收、锁不阻塞、崩溃恢复。"""
from __future__ import annotations

import io
import os
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import config
from app.db import LOCK, get_conn
from app.main import app
from app.merger import manager

client = TestClient(app)


@pytest.fixture(autouse=True)
def _isolated_store():
    """每个用例从空库开始，避免被篡改/删除的文件泄漏到后续合并场景。"""
    conn = get_conn()
    with LOCK:
        conn.execute("DELETE FROM inspection_files")
        conn.execute("DELETE FROM inspections")
        conn.execute("DELETE FROM merge_files")
        conn.execute("DELETE FROM merges")
        conn.execute("DELETE FROM cursor_positions")
        conn.execute("DELETE FROM cursor_files")
        conn.execute("DELETE FROM cursors")
        conn.execute("DELETE FROM files")
        conn.commit()
    for d in (config.FILES_DIR, config.TMP_DIR):
        if os.path.isdir(d):
            for name in os.listdir(d):
                try:
                    os.remove(os.path.join(d, name))
                except OSError:
                    pass
    yield


def _parquet(rows: int, tag: int) -> bytes:
    base = tag * 10_000
    stations = [f"ST-{(i + tag) % 4 + 1:03d}" for i in range(rows)]
    ts = [pa.scalar(base + i, type=pa.timestamp("us", tz="UTC")).as_py()
          for i in range(rows)]
    temps = [20.0 + (i % 7) + tag for i in range(rows)]
    table = pa.table({
        "station_id": pa.array(stations, pa.string()),
        "observed_at": pa.array(ts, pa.timestamp("us", tz="UTC")),
        "temperature": pa.array(temps, pa.float64()),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _wait_inspection(iid: int, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = client.get(f"/inspections/{iid}").json()
        if last["state"] in ("completed", "failed"):
            return last
        time.sleep(0.05)
    raise AssertionError(f"inspection {iid} did not finish: {last}")


def _wait_merge_terminal(timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = client.get("/merges/status").json()["active_merge"]
        if m is None or m["state"] in ("completed", "failed", "cancelled"):
            return
        time.sleep(0.05)
    raise AssertionError("merge never reached terminal state")


def _close_all_cursors() -> None:
    for c in client.get("/cursors").json()["cursors"]:
        if not c["closed"]:
            client.post(f"/cursors/{c['cursor_id']}/close")


def _start_merge() -> None:
    for _ in range(3):
        r = client.post("/merges")
        if r.status_code == 202:
            return
        _wait_merge_terminal()
    raise AssertionError("cannot start merge")


def _status_map(insp: dict) -> dict[str, dict]:
    return {f["file_id"]: f for f in insp["files"]}


def test_upload_registers_sha256_and_clean_inspection_is_intact():
    _wait_merge_terminal()
    data = _parquet(20, 1)
    info = client.post("/uploads", content=data).json()
    assert info["sha256"] and len(info["sha256"]) == 64

    # /files 清单也带指纹
    listed = next(f for f in client.get("/files").json()["files"]
                  if f["file_id"] == info["file_id"])
    assert listed["sha256"] == info["sha256"]

    r = client.post("/inspections")
    assert r.status_code == 202
    insp = _wait_inspection(r.json()["inspection_id"])
    assert insp["state"] == "completed"
    entry = _status_map(insp)[info["file_id"]]
    assert entry["status"] == "intact"
    assert entry["actual_sha256"] == info["sha256"]
    assert insp["summary"]["intact"] >= 1
    # 清单接口可见
    ids = [i["inspection_id"] for i in client.get("/inspections").json()["inspections"]]
    assert insp["inspection_id"] in ids


def test_tampered_file_reports_mismatch():
    data = _parquet(15, 2)
    info = client.post("/uploads", content=data).json()
    path = next(f["path"] for f in _all_files_row() if f["file_id"] == info["file_id"])

    # 直接改写正式产物的字节（模拟被误改）
    with open(path, "r+b") as fh:
        fh.seek(0)
        fh.write(b"XXXXXXXX")

    insp = _wait_inspection(
        client.post("/inspections").json()["inspection_id"])
    entry = _status_map(insp)[info["file_id"]]
    assert entry["status"] == "mismatch"
    assert entry["actual_sha256"] != entry["expected_sha256"]
    assert insp["summary"]["mismatch"] >= 1


def test_missing_disk_file_reports_missing_but_kept_registration():
    data = _parquet(12, 3)
    info = client.post("/uploads", content=data).json()
    path = next(f["path"] for f in _all_files_row() if f["file_id"] == info["file_id"])
    os.remove(path)  # 登记行保留，仅磁盘文件丢失

    insp = _wait_inspection(
        client.post("/inspections").json()["inspection_id"])
    entry = _status_map(insp)[info["file_id"]]
    assert entry["status"] == "missing"
    assert entry["actual_sha256"] is None
    assert insp["summary"]["missing"] >= 1


def test_legacy_row_without_fingerprint_is_unverified_never_intact():
    data = _parquet(10, 4)
    info = client.post("/uploads", content=data).json()
    conn = get_conn()
    with LOCK:
        conn.execute("UPDATE files SET sha256=NULL WHERE file_id=?",
                     (info["file_id"],))
        conn.commit()

    insp = _wait_inspection(
        client.post("/inspections").json()["inspection_id"])
    entry = _status_map(insp)[info["file_id"]]
    assert entry["status"] == "unverified"
    # 无指纹文件绝不能计入完好
    assert entry["status"] != "intact"
    assert insp["summary"]["unverified"] >= 1


def _block_inspection_hashing():
    """阻塞巡检线程的哈希计算，返回(已进入事件, 放行事件, 恢复函数)。

    只挂巡检管理器的测试钩子，不影响上传/合并自身调用的
    storage.sha256_file，避免把并发上传也一起堵死。
    """
    from app.inspections import manager as im
    entered = threading.Event()
    release = threading.Event()

    def hook(_path: str) -> None:
        entered.set()
        assert release.wait(timeout=15), "release gate never set"

    im.before_hash_hook = hook

    def restore():
        im.before_hash_hook = None

    return entered, release, restore


def test_legal_reclaim_during_inspection_is_reclaimed_not_missing():
    """巡检期间合并切换 + 回收：被合法删除的文件报 reclaimed，绝不报 missing。"""
    _wait_merge_terminal()
    # 两个 active 文件，受理后它们都进入巡检快照
    f1 = client.post("/uploads", content=_parquet(8, 10)).json()
    f2 = client.post("/uploads", content=_parquet(8, 11)).json()

    entered, release, restore = _block_inspection_hashing()
    try:
        created = client.post("/inspections").json()
        iid = created["inspection_id"]
        assert entered.wait(timeout=10), "inspection hashing never started"
        snap_uids = {f["file_id"] for f in created["files"]}
        assert {f1["file_id"], f2["file_id"]} <= snap_uids

        # 巡检阻塞在第一个文件的哈希上时，制造一次完整的合并 + 回收：
        # 新上传 -> 合并（f1/f2 变 obsolete）-> 关闭游标 -> 回收（登记行删除）。
        # 合并切换事务提交后会自动回收无引用旧件，显式 reclaim 再兜底一次。
        client.post("/uploads", content=_parquet(6, 12))
        _start_merge()
        _wait_merge_terminal()
        _close_all_cursors()
        client.post("/merges/reclaim")
        # f1/f2 的登记行确实已不存在（被合法回收，而不是仍登记在案）
        remaining = {f["file_id"] for f in client.get("/files").json()["files"]}
        assert f1["file_id"] not in remaining and f2["file_id"] not in remaining

        release.set()
    finally:
        restore()

    insp = _wait_inspection(iid)
    statuses = {f["status"] for f in insp["files"]}
    # 关键断言：合法回收绝不误报缺失
    assert "missing" not in statuses
    assert statuses == {"reclaimed"}
    assert insp["summary"]["reclaimed"] == len(created["files"])


def test_inspection_does_not_block_concurrent_upload_and_paging():
    """哈希计算不长期占住写锁：巡检期间上传与游标分页可继续提交。"""
    _wait_merge_terminal()
    seed = client.post("/uploads", content=_parquet(30, 20)).json()
    cur = client.post("/cursors").json()
    cid = cur["cursor_id"]

    entered, release, restore = _block_inspection_hashing()
    try:
        r = client.post("/inspections")
        iid = r.json()["inspection_id"]
        assert entered.wait(timeout=10), "inspection hashing never started"
        # 哈希窗口内：上传写事务必须能立刻提交（不被写锁挡住）
        concurrent = client.post("/uploads", content=_parquet(9, 21))
        assert concurrent.status_code == 200
        # 游标分页写事务同样不受阻
        page = client.get(f"/cursors/{cid}/pages?limit=5").json()
        assert page["returned"] == 5
        release.set()
    finally:
        restore()

    insp = _wait_inspection(iid)
    assert insp["state"] == "completed"
    # 巡检受理在并发上传之前，新文件不在本次快照内（受理即固定清单）
    snap = {f["file_id"] for f in insp["files"]}
    assert seed["file_id"] in snap
    assert concurrent.json()["file_id"] not in snap
    # 之后的新巡检能看到并发上传的文件且完好
    later = _wait_inspection(
        client.post("/inspections").json()["inspection_id"])
    assert _status_map(later)[concurrent.json()["file_id"]]["status"] == "intact"


def test_inspection_never_blocks_reclaim():
    """巡检快照不持有 files 外键引用：obsolete 文件回收不受巡检影响。"""
    _wait_merge_terminal()
    _close_all_cursors()
    client.post("/merges/reclaim")
    f1 = client.post("/uploads", content=_parquet(7, 30)).json()
    cid = client.post("/cursors").json()["cursor_id"]
    _start_merge()
    _wait_merge_terminal()

    # 发起巡检（会引用当时的 active merged 产物的快照行）
    client.post("/inspections")
    time.sleep(0.2)
    # 关闭最后一个游标引用，回收应照常成功，不被巡检挡住
    client.post(f"/cursors/{cid}/close")
    rec = client.post("/merges/reclaim").json()
    remaining = client.get("/files").json()["files"]
    disk = set(os.listdir(config.FILES_DIR))
    assert disk == {f["file_name"] for f in remaining}
    # 原上传文件已不在登记中
    assert all(f["file_id"] != f1["file_id"] for f in remaining)
    assert isinstance(rec["reclaimed"], list)


def test_interrupted_inspection_marked_failed_on_restart():
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO inspections(state, error) VALUES ('running', NULL)")
    stale_id = cur.lastrowid
    conn.execute(
        "INSERT INTO inspection_files(inspection_id, file_pk, file_uid, "
        "file_name, path, kind, expected_sha256, status, position) "
        "VALUES (?, 1, 'ghost', 'ghost.parquet', '/nope', 'upload', NULL, "
        "'pending', 0)", (stale_id,))
    conn.commit()

    from app.inspections import manager as im
    result = im.recover_on_startup()
    assert stale_id in result["interrupted_marked_failed"]
    state = conn.execute("SELECT state FROM inspections WHERE id=?",
                         (stale_id,)).fetchone()["state"]
    assert state == "failed"
    # 自动补开一次新巡检并能跑完
    if result["restarted"]:
        new_id = result["restarted"][0]
        done = _wait_inspection(new_id)
        assert done["state"] == "completed"


def test_unknown_inspection_returns_404():
    assert client.get("/inspections/999999").status_code == 404


def _all_files_row():
    """直接读库拿 path（测试辅助，避免在 API 响应里暴露绝对路径）。"""
    conn = get_conn()
    return conn.execute(
        "SELECT file_id, path FROM files").fetchall()

"""完整性巡检：对正式 Parquet 产物做 SHA-256 核验。

设计要点（对应交付契约）：

- **受理即固定清单**：创建巡检的事务里把当时所有 ``status='active'`` 的文件
  快照写入 ``inspection_files``，之后合并切换、回收、新上传都不影响本次清单。
- **四种核验结论 + 合法回收**：
    * ``intact``     磁盘存在且重新计算的 SHA-256 与登记指纹一致；
    * ``mismatch``   磁盘存在但指纹不一致（正式产物被误改）；
    * ``missing``    登记行仍在、磁盘文件却没了（正式产物丢失）；
    * ``unverified`` 登记行没有指纹（旧版本登记的历史文件），绝不能报完好；
    * ``reclaimed``  巡检期间合并切换或旧件回收合法删除了该登记行，
      与“丢失”严格区分，绝不误报 missing。
- **不长期占住 SQLite 写锁**：每个文件的 SHA-256 都在锁外以流式方式读取
  计算，期间上传与游标分页的写事务可以正常提交；只在写结论时取锁短提交。
- **巡检不阻碍回收**：快照表不建指向 ``files`` 的外键，任何巡检（含失败、
  中断、长期未查看）都不会持有引用，旧件回收照常进行。
"""
from __future__ import annotations

import os
import threading
from datetime import datetime, timezone

from . import storage
from .db import LOCK, get_conn

# 巡检结论 -> 中文说明（/docs 与响应体同时给出英文状态与中文描述）
STATUS_TEXT = {
    "pending": "待核验",
    "intact": "完好",
    "mismatch": "内容不符",
    "missing": "缺失",
    "unverified": "未核验（登记时无指纹）",
    "reclaimed": "合法回收（合并切换或旧件回收）",
    "error": "核验出错",
}

# 汇总计数时关心的终态
TRACKED_STATUSES = ("intact", "mismatch", "missing", "unverified", "reclaimed")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class InspectionManager:
    def __init__(self) -> None:
        self._threads: dict[int, threading.Thread] = {}
        # 测试钩子：巡检线程每次哈希前调用（默认 None，生产路径零开销）。
        # 刻意挂在巡检管理器而不是 storage 上，避免误伤上传/合并自身的哈希。
        self.before_hash_hook = None

    # ----------------------------- 受理 -----------------------------

    def start_inspection(self) -> dict:
        """受理一次巡检：事务内固定当前 active 文件清单并启动后台核验。"""
        conn = get_conn()
        with LOCK:
            active_files = conn.execute(
                """SELECT id, file_id, path, kind, sha256
                   FROM files
                   WHERE status='active'
                   ORDER BY read_order, seq_order"""
            ).fetchall()
            cur = conn.execute(
                "INSERT INTO inspections(state, error) VALUES ('running', NULL)"
            )
            inspection_id = int(cur.lastrowid)
            for pos, f in enumerate(active_files):
                conn.execute(
                    """INSERT INTO inspection_files
                       (inspection_id, file_pk, file_uid, file_name, path, kind,
                        expected_sha256, status, position)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
                    (
                        inspection_id,
                        f["id"],
                        f["file_id"],
                        f["path"].rsplit("/", 1)[-1],
                        f["path"],
                        f["kind"],
                        f["sha256"],
                        pos,
                    ),
                )
            conn.commit()

        self._spawn_thread(inspection_id)
        return self.get_inspection(inspection_id)

    def _spawn_thread(self, inspection_id: int) -> None:
        t = threading.Thread(
            target=self._run_inspection,
            args=(inspection_id,),
            name=f"inspection-{inspection_id}",
            daemon=True,
        )
        self._threads[inspection_id] = t
        t.start()

    # --------------------------- 后台核验 ---------------------------

    def _run_inspection(self, inspection_id: int) -> None:
        conn = get_conn()
        try:
            # 重新读取自己的快照清单（快照本身不受后续 files 变更影响）
            with LOCK:
                snapshot = conn.execute(
                    """SELECT file_pk, path, expected_sha256
                       FROM inspection_files
                       WHERE inspection_id=? ORDER BY position""",
                    (inspection_id,),
                ).fetchall()

            for item in snapshot:
                file_pk = item["file_pk"]

                # 第一步（锁外 IO）：看磁盘文件在不在。
                # 注意不能只看磁盘：合法回收会同时删登记行和磁盘文件，
                # 那是 reclaimed 而不是 missing。
                disk_exists = os.path.exists(item["path"])
                actual = None
                io_error = None
                if disk_exists:
                    try:
                        # 关键：哈希计算在任何数据库锁之外，流式读取，
                        # 不占 SQLite 写锁，上传/分页此刻可正常提交
                        if self.before_hash_hook is not None:
                            self.before_hash_hook(item["path"])
                        actual = storage.sha256_file(item["path"])
                    except OSError as exc:
                        io_error = str(exc)

                # 第二步（锁内短事务）：结合登记行现状判定结论
                with LOCK:
                    registered = conn.execute(
                        "SELECT id, sha256, status FROM files WHERE id=?",
                        (file_pk,),
                    ).fetchone()
                    if registered is None:
                        # 登记行已被合并切换/旧件回收合法删除
                        status, detail = (
                            "reclaimed",
                            "登记行已随合并切换或旧件回收合法删除",
                        )
                        actual = None
                    elif not disk_exists:
                        # 登记行还在，磁盘文件却没了 => 正式产物丢失
                        status, detail = (
                            "missing",
                            "登记仍在但磁盘文件不存在",
                        )
                    elif io_error is not None:
                        status, detail = "error", f"读取失败: {io_error}"
                    elif registered["sha256"] is None:
                        # 旧文件：已登记却没有指纹，不能报完好
                        status, detail = (
                            "unverified",
                            "文件登记中没有 SHA-256 指纹，无法核验完整性",
                        )
                    elif actual == registered["sha256"]:
                        status, detail = "intact", "SHA-256 与登记指纹一致"
                    else:
                        status, detail = (
                            "mismatch",
                            "重新计算的 SHA-256 与登记指纹不一致，内容疑似被篡改",
                        )
                    conn.execute(
                        """UPDATE inspection_files
                           SET status=?, actual_sha256=?, detail=?
                           WHERE inspection_id=? AND file_pk=?""",
                        (status, actual, detail, inspection_id, file_pk),
                    )
                    conn.commit()

            with LOCK:
                conn.execute(
                    "UPDATE inspections SET state='completed', finished_at=? "
                    "WHERE id=?",
                    (_now(), inspection_id),
                )
                conn.commit()
        except Exception as exc:  # noqa: BLE001 - 后台线程兜底，失败也要落库
            with LOCK:
                conn.execute(
                    "UPDATE inspections SET state='failed', error=?, "
                    "finished_at=? WHERE id=?",
                    (str(exc), _now(), inspection_id),
                )
                conn.commit()

    # ----------------------------- 查询 -----------------------------

    def _serialize_file(self, r) -> dict:
        return {
            "position": r["position"],
            "file_pk": r["file_pk"],
            "file_id": r["file_uid"],
            "file_name": r["file_name"],
            "kind": r["kind"],
            "expected_sha256": r["expected_sha256"],
            "actual_sha256": r["actual_sha256"],
            "status": r["status"],
            "status_text": STATUS_TEXT.get(r["status"], r["status"]),
            "detail": r["detail"],
        }

    def get_inspection(self, inspection_id: int) -> dict:
        conn = get_conn()
        with LOCK:
            head = conn.execute(
                "SELECT * FROM inspections WHERE id=?", (inspection_id,)
            ).fetchone()
            if head is None:
                raise InspectionNotFound(inspection_id)
            files = conn.execute(
                """SELECT * FROM inspection_files
                   WHERE inspection_id=? ORDER BY position""",
                (inspection_id,),
            ).fetchall()
            counts = {s: 0 for s in TRACKED_STATUSES}
            for r in files:
                if r["status"] in counts:
                    counts[r["status"]] += 1
        return {
            "inspection_id": head["id"],
            "state": head["state"],
            "error": head["error"],
            "created_at": head["created_at"],
            "finished_at": head["finished_at"],
            "snapshot_files": len(files),
            "summary": counts,
            "files": [self._serialize_file(r) for r in files],
        }

    def list_inspections(self) -> dict:
        conn = get_conn()
        with LOCK:
            rows = conn.execute(
                "SELECT id, state, error, created_at, finished_at, "
                "(SELECT COUNT(*) FROM inspection_files ifl "
                " WHERE ifl.inspection_id = i.id) AS snapshot_files "
                "FROM inspections i ORDER BY id DESC"
            ).fetchall()
        return {
            "inspections": [
                {
                    "inspection_id": r["id"],
                    "state": r["state"],
                    "error": r["error"],
                    "created_at": r["created_at"],
                    "finished_at": r["finished_at"],
                    "snapshot_files": r["snapshot_files"],
                }
                for r in rows
            ]
        }

    # --------------------------- 崩溃恢复 ---------------------------

    def recover_on_startup(self) -> dict:
        """进程重启时，把中断在 running 的巡检标记 failed 并续跑一次。

        巡检是只读核验，重跑没有副作用；中断任务不允许永远挂 running。
        快照行保留（无外键、不阻碍回收），结果仍可回看。
        """
        conn = get_conn()
        resumed: list[int] = []
        with LOCK:
            stale = conn.execute(
                "SELECT id FROM inspections WHERE state='running' ORDER BY id"
            ).fetchall()
            for r in stale:
                conn.execute(
                    "UPDATE inspections SET state='failed', error=?, finished_at=? "
                    "WHERE id=?",
                    ("进程在巡检过程中退出，重启后需重新发起巡检", _now(), r["id"]),
                )
            conn.commit()
        # 失败结论落库后另起一次全新巡检，恢复“后台持续核验”的语义
        if stale:
            latest = self.start_inspection()
            resumed.append(latest["inspection_id"])
        return {"interrupted_marked_failed": [r["id"] for r in stale],
                "restarted": resumed}

    def shutdown(self, timeout: float = 5.0) -> None:
        for t in list(self._threads.values()):
            t.join(timeout=timeout)


class InspectionNotFound(RuntimeError):
    """巡检不存在，API 层映射为 404。"""


manager = InspectionManager()

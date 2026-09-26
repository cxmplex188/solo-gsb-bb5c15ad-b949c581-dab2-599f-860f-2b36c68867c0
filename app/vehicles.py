"""应急车辆申请 —— 存储与受理域（不依赖 fastapi，便于无第三方依赖测试）。

与 app.main 同一把 state_lock、同一个 SQLite 连接下被调用，因此本模块
不再自行加锁，只负责：

- 车辆池：每个房间至多一个，总量 1～20，创建后固定不变（重置演练也保留）；
- 组申请：每个参与组在池内至多一条申请，数量 1～5 且不超过车辆池总量，
  取消（删除）后才能重新申请；
- 受理顺序：提交申请时，若无更早的待分配（排队中）申请且余量足够，则
  立即分配；否则入队。队列按受理顺序（queue_seq 房间内单调递增）排列；
- 队首补位：取消（或删除参与组级联）释放车辆后，从队首依次补位——
  队首数量不足时立即停止，绝不跳过队首分配给后面的申请；
- 重置演练：清空全部申请并恢复全部余量（全部车辆重新可用），车辆池保留。

状态完全由 vehicle_pools / vehicle_requests 两行持久化表推导，刷新、
断线重连与服务重启后仍一致；不依赖虚拟时钟，暂停 / 继续 / 重置之外的
时钟动作不影响受理。
"""

from __future__ import annotations

import sqlite3
import time

MIN_TOTAL = 1
MAX_TOTAL = 20
MIN_AMOUNT = 1
MAX_AMOUNT = 5

STATUS_QUEUED = 0
STATUS_ALLOCATED = 1


class VehicleError(Exception):
    """车辆申请域校验错误。main.py 捕获后转成对应 HTTP 状态码。"""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


# ---------------------------------------------------------------- 车辆池

def create_pool(
    conn: sqlite3.Connection,
    room_id: int,
    total: int,
    *,
    now: float | None = None,
) -> sqlite3.Row:
    """为本房间创建车辆池。调用方须先校验 total 为 1～20 的整数；
    房间已有车辆池时抛 409（车辆池总量固定，唯一且不可改）。"""
    now = time.time() if now is None else now
    if conn.execute(
        "SELECT 1 FROM vehicle_pools WHERE room_id = ?", (room_id,)
    ).fetchone():
        raise VehicleError(409, "vehicle pool already exists")
    cur = conn.execute(
        "INSERT INTO vehicle_pools (room_id, total, created_at) VALUES (?, ?, ?)",
        (room_id, total, now),
    )
    conn.commit()
    return conn.execute("SELECT * FROM vehicle_pools WHERE id = ?", (cur.lastrowid,)).fetchone()


def get_pool(conn: sqlite3.Connection, room_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM vehicle_pools WHERE room_id = ?", (room_id,)
    ).fetchone()


# ---------------------------------------------------------------- 读取

def allocated_count(conn: sqlite3.Connection, room_id: int) -> int:
    """当前已分配车辆总数 = 已分配申请的数量之和。"""
    row = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) AS s FROM vehicle_requests"
        " WHERE room_id = ? AND status = ?",
        (room_id, STATUS_ALLOCATED),
    ).fetchone()
    return int(row["s"])


def remaining_count(conn: sqlite3.Connection, room_id: int, total: int | None = None) -> int:
    """车辆池余量。total 为 None 时现查车辆池（无池返回 0）。"""
    if total is None:
        pool = get_pool(conn, room_id)
        if pool is None:
            return 0
        total = pool["total"]
    return max(0, total - allocated_count(conn, room_id))


def request_of_group(conn: sqlite3.Connection, room_id: int, group_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM vehicle_requests WHERE room_id = ? AND group_id = ?",
        (room_id, group_id),
    ).fetchone()


def list_requests(conn: sqlite3.Connection, room_id: int) -> list[sqlite3.Row]:
    """全部申请，先按状态（已分配在前，便于主持台汇总）再按受理顺序列出。"""
    return conn.execute(
        "SELECT * FROM vehicle_requests WHERE room_id = ?"
        " ORDER BY status DESC, queue_seq",
        (room_id,),
    ).fetchall()


def list_queued(conn: sqlite3.Connection, room_id: int) -> list[sqlite3.Row]:
    """排队中的申请，严格按受理顺序（队首 = queue_seq 最小）。"""
    return conn.execute(
        "SELECT * FROM vehicle_requests WHERE room_id = ? AND status = ?"
        " ORDER BY queue_seq",
        (room_id, STATUS_QUEUED),
    ).fetchall()


def queue_position(conn: sqlite3.Connection, request_id: int) -> int | None:
    """申请在队列中的排位（队首为 1）；非排队中申请返回 None。"""
    row = conn.execute(
        "SELECT room_id FROM vehicle_requests WHERE id = ? AND status = ?",
        (request_id, STATUS_QUEUED),
    ).fetchone()
    if row is None:
        return None
    n = conn.execute(
        "SELECT COUNT(*) AS c FROM vehicle_requests"
        " WHERE room_id = ? AND status = ? AND queue_seq <="
        " (SELECT queue_seq FROM vehicle_requests WHERE id = ?)",
        (row["room_id"], STATUS_QUEUED, request_id),
    ).fetchone()["c"]
    return int(n)


# ---------------------------------------------------------------- 受理 / 取消

def submit_request(
    conn: sqlite3.Connection,
    room_id: int,
    group_id: int,
    amount: int,
    *,
    now: float | None = None,
) -> sqlite3.Row:
    """参与组提交一份申请（1～5 辆且不超过车辆池总量）。

    受理规则（在调用方持有的 state_lock 内串行执行）：无更早的待分配申请
    且余量足够时立即分配；否则入队等待补位。每组至多一条申请，已有申请
    （排队中或已分配）时抛 409——必须先取消。
    """
    now = time.time() if now is None else now
    pool = get_pool(conn, room_id)
    if pool is None:
        raise VehicleError(404, "vehicle pool not found")
    if not isinstance(amount, int) or amount < MIN_AMOUNT or amount > MAX_AMOUNT:
        raise VehicleError(400, f"amount must be {MIN_AMOUNT}~{MAX_AMOUNT}")
    if amount > pool["total"]:
        raise VehicleError(400, "amount exceeds vehicle pool total")
    if request_of_group(conn, room_id, group_id) is not None:
        raise VehicleError(409, "existing request must be cancelled first")
    seq_row = conn.execute(
        "SELECT COALESCE(MAX(queue_seq), 0) + 1 AS s FROM vehicle_requests WHERE room_id = ?",
        (room_id,),
    ).fetchone()
    # 无更早待分配申请（队列为空）且余量足够 → 立即分配，否则入队
    immediate = remaining_count(conn, room_id, pool["total"]) >= amount and not list_queued(conn, room_id)
    cur = conn.execute(
        "INSERT INTO vehicle_requests"
        " (room_id, group_id, amount, status, queue_seq, created_at, allocated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (room_id, group_id, amount,
         STATUS_ALLOCATED if immediate else STATUS_QUEUED,
         seq_row["s"], now, now if immediate else None),
    )
    conn.commit()
    return conn.execute(
        "SELECT * FROM vehicle_requests WHERE id = ?", (cur.lastrowid,)
    ).fetchone()


def drain_queue(conn: sqlite3.Connection, room_id: int, *, now: float | None = None) -> list[int]:
    """取消已分配申请（或删除参与组）释放车辆后，从队首依次补位。

    严格 FIFO：按受理顺序逐条尝试，队首数量不足则立即停止——后续申请
    即使数量更小也绝不跳过队首。返回本次新分配的申请 id 列表（受理顺序）。
    幂等：无变化时返回空列表；重启后重复调用也不会重复分配（只取 queued 行）。
    """
    now = time.time() if now is None else now
    pool = get_pool(conn, room_id)
    if pool is None:
        return []
    allocated: list[int] = []
    for req in list_queued(conn, room_id):
        if remaining_count(conn, room_id, pool["total"]) < req["amount"]:
            break  # 队首不足：停止，绝不跳过
        conn.execute(
            "UPDATE vehicle_requests SET status = ?, allocated_at = ? WHERE id = ? AND status = ?",
            (STATUS_ALLOCATED, now, req["id"], STATUS_QUEUED),
        )
        conn.commit()
        allocated.append(req["id"])
    return allocated


def cancel_request(conn: sqlite3.Connection, room_id: int, group_id: int) -> sqlite3.Row:
    """取消本组申请（排队中或已分配均可）。调用方负责随后 drain_queue 补位。

    申请不存在时抛 404。行被物理删除（取消后才能重新申请，UNIQUE 约束即
    申请唯一凭证）；申请与分配历史仍完整保留在 events 事件流中。
    """
    req = request_of_group(conn, room_id, group_id)
    if req is None:
        raise VehicleError(404, "vehicle request not found")
    conn.execute("DELETE FROM vehicle_requests WHERE id = ?", (req["id"],))
    conn.commit()
    return req


# ---------------------------------------------------------------- 重置

def reset_requests(conn: sqlite3.Connection, room_id: int) -> int:
    """重置演练：清空全部申请，车辆池保留（余量随申请删除自然恢复为总量）。

    返回被删除的申请条数。"""
    cur = conn.execute("DELETE FROM vehicle_requests WHERE room_id = ?", (room_id,))
    conn.commit()
    return cur.rowcount

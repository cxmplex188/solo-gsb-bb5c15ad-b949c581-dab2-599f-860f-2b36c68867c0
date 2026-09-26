"""应急车辆申请服务端语义的无第三方依赖验证（python3 tests/test_vehicles.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），而是直接在
app.db 建表的 SQLite 上调用 app.vehicles 域函数，复刻 main.py 路由在
state_lock 临界区内对车辆池/申请的操作次序，校验：

1. 车辆池：每房间至多一个，重复创建 409；总量 1～20 由路由层 Field 约束，
   域层拒绝越界（1～5 申请上限、不得超过池总量）；
2. 单组申请：每组至多一份，重复提交 409，取消后可重新申请；
3. 立即分配：池余量足够且队列为空时立即 allocated；
4. 入队：余量不足（或前面已有排队）时 queued，排位从 1 起；
5. 队首补位：取消已分配申请后从队首依次补位；
6. 队首不足绝不跳过：队首要 5、余量 3 时不分配，后面 2 辆的也不跳过；
7. 取消排队中申请：从队中移除，其后排位前移，不触发新分配；
8. 重置：清空全部申请、恢复全部余量，车辆池保留；
9. 删除参与组/房间：级联清理申请，删组释放后队首补位；
10. 重启一致性：状态全部由持久化行推导，重开连接后余量/排位不变。
"""
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db as dbmod  # noqa: E402
from app import vehicles as vm  # noqa: E402


def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(dbmod.SCHEMA)
    return conn


def make_room(conn, code="VEH001"):
    rid = conn.execute(
        "INSERT INTO rooms (code,name,status,elapsed_ms,created_at)"
        " VALUES (?,?, 'idle', 0, ?)",
        (code, "车辆房", time.time()),
    ).lastrowid
    conn.commit()
    return rid


def add_group(conn, rid, name, code):
    gid = conn.execute(
        "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
        (rid, name, code, time.time()),
    ).lastrowid
    conn.commit()
    return gid


def expect_error(status, fn, *args):
    try:
        fn(*args)
    except vm.VehicleError as e:
        assert e.status_code == status, (status, e.status_code, e.detail)
        return
    raise AssertionError(f"expected VehicleError {status}")


def test_pool_unique_and_amount_bounds():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        pool = vm.create_pool(conn, rid, 20)
        assert pool["total"] == 20
        expect_error(409, vm.create_pool, conn, rid, 10)  # 每房间至多一个
        g1 = add_group(conn, rid, "红队", "VV0001")
        # 申请 1～5 辆
        expect_error(400, vm.submit_request, conn, rid, g1, 0)
        expect_error(400, vm.submit_request, conn, rid, g1, 6)
        # 不得超过池总量：换一个小池房间验证
        rid2 = make_room(conn, "VEH002")
        vm.create_pool(conn, rid2, 3)
        gx = add_group(conn, rid2, "蓝队", "VV0002")
        expect_error(400, vm.submit_request, conn, rid2, gx, 5)  # 5 > 池总量 3
        ok = vm.submit_request(conn, rid2, gx, 3)
        assert ok["status"] == vm.STATUS_ALLOCATED
        conn.close()
    print("ok: pool unique per room; amount 1~5 and <= pool total enforced")


def test_one_request_per_group_and_recancel():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        vm.create_pool(conn, rid, 5)
        g1 = add_group(conn, rid, "红队", "VV0011")
        g2 = add_group(conn, rid, "蓝队", "VV0012")
        r1 = vm.submit_request(conn, rid, g1, 4)
        assert r1["status"] == vm.STATUS_ALLOCATED
        # 已有申请（已分配）：重复提交 409
        expect_error(409, vm.submit_request, conn, rid, g1, 1)
        # 排队中的申请同样占位：另一组入队后重复提交也 409
        vm.submit_request(conn, rid, g2, 3)  # 余量 1 < 3 → 排队
        expect_error(409, vm.submit_request, conn, rid, g2, 1)
        # 取消后可重新申请；取消排队中申请不释放余量，但新申请（1 辆，余量够
        # 且队列已空）立即分配
        vm.cancel_request(conn, rid, g2)
        r2 = vm.submit_request(conn, rid, g2, 1)
        assert r2["status"] == vm.STATUS_ALLOCATED and r2["amount"] == 1
        # 取消不存在的申请 404
        g3 = add_group(conn, rid, "绿队", "VV0013")
        expect_error(404, vm.cancel_request, conn, rid, g3)
        conn.close()
    print("ok: one active request per group; reapply only after cancel")


def test_immediate_allocation_when_room_and_supply():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        vm.create_pool(conn, rid, 10)
        g1 = add_group(conn, rid, "红队", "VV0021")
        g2 = add_group(conn, rid, "蓝队", "VV0022")
        r1 = vm.submit_request(conn, rid, g1, 5)
        r2 = vm.submit_request(conn, rid, g2, 5)
        # 队列为空且余量足够（5+5=10）：两条都立即分配
        assert r1["status"] == r2["status"] == vm.STATUS_ALLOCATED
        assert vm.remaining_count(conn, rid) == 0
        assert vm.queue_position(conn, r1["id"]) is None
        conn.close()
    print("ok: allocate immediately when queue empty and enough remaining")


def test_enqueue_when_insufficient_and_positions():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        vm.create_pool(conn, rid, 7)
        g1 = add_group(conn, rid, "红队", "VV0031")
        g2 = add_group(conn, rid, "蓝队", "VV0032")
        g3 = add_group(conn, rid, "绿队", "VV0033")
        vm.submit_request(conn, rid, g1, 5)           # 立即分配，余量 2
        q2 = vm.submit_request(conn, rid, g2, 3)      # 余量 2 < 3 → 排队第 1
        # g3 提交时余量虽然够（2 >= 2），但存在更早待分配申请 → 必须排在后面
        q3 = vm.submit_request(conn, rid, g3, 2)
        assert q2["status"] == vm.STATUS_QUEUED and vm.queue_position(conn, q2["id"]) == 1
        assert q3["status"] == vm.STATUS_QUEUED and vm.queue_position(conn, q3["id"]) == 2
        # 余量与已分配不受排队影响
        assert vm.remaining_count(conn, rid) == 2
        conn.close()
    print("ok: enqueue on insufficient supply or earlier pending; positions start at 1")


def test_fifo_backfill_on_cancel_allocated():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        vm.create_pool(conn, rid, 8)
        g1 = add_group(conn, rid, "红队", "VV0041")
        g2 = add_group(conn, rid, "蓝队", "VV0042")
        g3 = add_group(conn, rid, "绿队", "VV0043")
        vm.submit_request(conn, rid, g1, 5)
        q2 = vm.submit_request(conn, rid, g2, 4)   # 余量 3 < 4 → 排队首
        q3 = vm.submit_request(conn, rid, g3, 1)   # 队二：1 辆
        assert vm.queue_position(conn, q2["id"]) == 1
        # 取消已分配的 g1（释放 5 辆）→ 从队首依次补位：4 然后 1
        vm.cancel_request(conn, rid, g1)
        moved = vm.drain_queue(conn, rid)
        assert moved == [q2["id"], q3["id"]]
        assert vm.request_of_group(conn, rid, g2)["status"] == vm.STATUS_ALLOCATED
        assert vm.request_of_group(conn, rid, g3)["status"] == vm.STATUS_ALLOCATED
        assert vm.remaining_count(conn, rid) == 3          # 8 - 4 - 1
        # 幂等：再次 drain 无变化
        assert vm.drain_queue(conn, rid) == []
        conn.close()
    print("ok: FIFO backfill from queue head after cancelling allocated request")


def test_head_shortage_blocks_later_requests():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        vm.create_pool(conn, rid, 8)
        g0 = add_group(conn, rid, "白队", "VV0050")
        g1 = add_group(conn, rid, "红队", "VV0051")
        g2 = add_group(conn, rid, "蓝队", "VV0052")
        g3 = add_group(conn, rid, "绿队", "VV0053")
        vm.submit_request(conn, rid, g0, 4)
        vm.submit_request(conn, rid, g1, 1)   # 已分配 5，余量 3
        q_head = vm.submit_request(conn, rid, g2, 5)  # 队首要 5（余量 3，不足）
        q_tail = vm.submit_request(conn, rid, g3, 2)  # 队二只要 2（余量 3 本够）
        # 队首 5 > 余量 3：队首不分配，队二 2 也绝不允许跳过
        assert vm.drain_queue(conn, rid) == []
        assert vm.request_of_group(conn, rid, g2)["status"] == vm.STATUS_QUEUED
        assert vm.request_of_group(conn, rid, g3)["status"] == vm.STATUS_QUEUED
        assert vm.queue_position(conn, q_head["id"]) == 1
        assert vm.queue_position(conn, q_tail["id"]) == 2
        assert vm.remaining_count(conn, rid) == 3
        # 取消一个已分配组（释放 1，余量 4）：队首要 5 仍不足，依旧不得跳过队首
        vm.cancel_request(conn, rid, g1)
        assert vm.drain_queue(conn, rid) == []
        assert vm.request_of_group(conn, rid, g3)["status"] == vm.STATUS_QUEUED
        assert vm.remaining_count(conn, rid) == 4
        # 再取消 g0（释放 4，余量 8）：严格按队首先 5 后 2 依次补位
        vm.cancel_request(conn, rid, g0)
        moved = vm.drain_queue(conn, rid)
        assert moved == [q_head["id"], q_tail["id"]]
        assert vm.request_of_group(conn, rid, g2)["status"] == vm.STATUS_ALLOCATED
        assert vm.request_of_group(conn, rid, g3)["status"] == vm.STATUS_ALLOCATED
        assert vm.remaining_count(conn, rid) == 1
        conn.close()
    print("ok: queue head shortage blocks allocation; never skip head")


def test_cancel_queued_shifts_positions():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        vm.create_pool(conn, rid, 5)
        g1 = add_group(conn, rid, "红队", "VV0061")
        g2 = add_group(conn, rid, "蓝队", "VV0062")
        g3 = add_group(conn, rid, "绿队", "VV0063")
        g4 = add_group(conn, rid, "黄队", "VV0064")
        vm.submit_request(conn, rid, g1, 5)             # 占满
        q2 = vm.submit_request(conn, rid, g2, 1)
        q3 = vm.submit_request(conn, rid, g3, 1)
        vm.submit_request(conn, rid, g4, 1)
        assert [vm.queue_position(conn, q2["id"]),
                vm.queue_position(conn, q3["id"])] == [1, 2]
        # 取消队首 g2：g3 升为第 1 位；没有释放已分配车辆 → 无新分配
        vm.cancel_request(conn, rid, g2)
        assert vm.drain_queue(conn, rid) == []
        assert vm.queue_position(conn, q3["id"]) == 1
        assert vm.request_of_group(conn, rid, g3)["status"] == vm.STATUS_QUEUED
        assert vm.remaining_count(conn, rid) == 0
        conn.close()
    print("ok: cancel queued request shifts positions, no spurious allocation")


def test_reset_clears_requests_keeps_pool():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        pool = vm.create_pool(conn, rid, 8)
        g1 = add_group(conn, rid, "红队", "VV0071")
        g2 = add_group(conn, rid, "蓝队", "VV0072")
        vm.submit_request(conn, rid, g1, 5)
        vm.submit_request(conn, rid, g2, 5)  # 余量 3 不足 5 → 排队
        assert vm.remaining_count(conn, rid) == 3
        deleted = vm.reset_requests(conn, rid)
        assert deleted == 2
        assert vm.list_requests(conn, rid) == []
        assert vm.remaining_count(conn, rid) == 8    # 全部余量恢复
        # 车辆池保留，可直接重新申请
        assert vm.get_pool(conn, rid)["id"] == pool["id"]
        r = vm.submit_request(conn, rid, g1, 5)
        assert r["status"] == vm.STATUS_ALLOCATED
        # 重置幂等：无申请时返回 0
        assert vm.reset_requests(conn, rid) == 1
        assert vm.reset_requests(conn, rid) == 0
        conn.close()
    print("ok: reset clears requests, restores capacity, keeps pool")


def test_cascade_group_delete_backfills_and_room_delete():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        vm.create_pool(conn, rid, 7)
        g1 = add_group(conn, rid, "红队", "VV0081")
        g2 = add_group(conn, rid, "蓝队", "VV0082")
        vm.submit_request(conn, rid, g1, 4)
        q2 = vm.submit_request(conn, rid, g2, 4)  # 余量 3 不足 4 → 队首
        # 主持人删除参与组（外键级联删申请），随后路由会 drain_queue 补位
        conn.execute("DELETE FROM groups WHERE id=?", (g1,))
        conn.commit()
        moved = vm.drain_queue(conn, rid)
        assert moved == [q2["id"]]
        assert vm.remaining_count(conn, rid) == 3
        # 删除房间：车辆池与申请全部级联
        conn.execute("DELETE FROM rooms WHERE id=?", (rid,))
        conn.commit()
        assert conn.execute("SELECT COUNT(*) c FROM vehicle_pools").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM vehicle_requests").fetchone()["c"] == 0
        conn.close()
    print("ok: group delete cascades and backfills; room delete clears pool/requests")


def test_restart_state_derived_from_persisted_rows():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        rid = make_room(conn)
        vm.create_pool(conn, rid, 8)
        g1 = add_group(conn, rid, "红队", "VV0091")
        g2 = add_group(conn, rid, "蓝队", "VV0092")
        g3 = add_group(conn, rid, "绿队", "VV0093")
        vm.submit_request(conn, rid, g1, 5)
        q2 = vm.submit_request(conn, rid, g2, 5)   # 余量 3 不足 5 → 队首
        vm.submit_request(conn, rid, g3, 4)           # 队二（有更早排队，必须等待）
        conn.close()

        # 重启：重新连接并执行 init_schema（新表 IF NOT EXISTS 已在既有库中），
        # 余量/状态/排位全部由持久化行推导，不依赖任何内存队列
        conn2 = open_db(path)
        dbmod.init_schema(conn2)
        pool = vm.get_pool(conn2, rid)
        assert pool["total"] == 8
        assert vm.remaining_count(conn2, rid) == 3
        assert vm.request_of_group(conn2, rid, g1)["status"] == vm.STATUS_ALLOCATED
        assert vm.queue_position(conn2, q2["id"]) == 1
        assert vm.request_of_group(conn2, rid, g3)["status"] == vm.STATUS_QUEUED
        # 释放后补位行为在重启后仍然正确：队首要 5（取消释放 5 后恰够），
        # 补位后余量 3 < 队二的 4，队二继续排队（严格逐辆停止，不超分）
        vm.cancel_request(conn2, rid, g1)
        moved = vm.drain_queue(conn2, rid)
        assert moved == [q2["id"]]
        assert vm.request_of_group(conn2, rid, g3)["status"] == vm.STATUS_QUEUED
        assert vm.queue_position(conn2, vm.request_of_group(conn2, rid, g3)["id"]) == 1
        conn2.close()
    print("ok: restart keeps pool/supply/queue consistent; backfill still correct")


def test_schema_creates_vehicle_tables_and_unique_constraint():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        names = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"vehicle_pools", "vehicle_requests"} <= names
        indexes = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")}
        assert {"idx_vehicle_pools_room", "idx_vehicle_requests_room",
                "idx_vehicle_requests_group"} <= indexes
        rid = make_room(conn)
        vm.create_pool(conn, rid, 5)
        g1 = add_group(conn, rid, "红队", "VV0101")
        vm.submit_request(conn, rid, g1, 1)
        try:
            conn.execute(
                "INSERT INTO vehicle_requests (room_id,group_id,amount,status,queue_seq,created_at)"
                " VALUES (?,?,?,0,9,?)",
                (rid, g1, 2, time.time()))
            conn.commit()
            raise AssertionError("UNIQUE(room_id, group_id) 未生效")
        except sqlite3.IntegrityError:
            pass
        # 车辆池每房间唯一
        try:
            conn.execute("INSERT INTO vehicle_pools (room_id,total,created_at) VALUES (?,?,?)",
                         (rid, 9, time.time()))
            conn.commit()
            raise AssertionError("vehicle_pools.room_id UNIQUE 未生效")
        except sqlite3.IntegrityError:
            pass
        conn.close()
    print("ok: schema creates vehicle tables/indexes and UNIQUE constraints")


if __name__ == "__main__":
    test_pool_unique_and_amount_bounds()
    test_one_request_per_group_and_recancel()
    test_immediate_allocation_when_room_and_supply()
    test_enqueue_when_insufficient_and_positions()
    test_fifo_backfill_on_cancel_allocated()
    test_head_shortage_blocks_later_requests()
    test_cancel_queued_shifts_positions()
    test_reset_clears_requests_keeps_pool()
    test_cascade_group_delete_backfills_and_room_delete()
    test_restart_state_derived_from_persisted_rows()
    test_schema_creates_vehicle_tables_and_unique_constraint()
    print("ALL VEHICLE TESTS PASSED")

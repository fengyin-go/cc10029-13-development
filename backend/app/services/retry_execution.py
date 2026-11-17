"""失败重试 v2：幂等重试批次计算与重试执行数据集。

设计要点（对应验收口径）：

1. 失败采集批次按“幂等标识”去重：重复失败事件只累加事件计数，不会创建第二份
   重试执行记录；失败批次、幂等标识与执行数据集在同一事务内同时写入。
2. 退避时间、重试次数、状态记录全部挂在同一个任务批次（执行数据集记录）上统一管理。
3. 一批重试部分失败时只保留成功回执、把失败项补排，绝不整批覆盖既有执行结果。
4. 爬虫引擎消费的“重试任务分配清单”只投影尚未成功且未达上限的批次，并与执行数据集
   同事务提交，执行数据集写入失败时一起回滚。
5. 人工重试与定时重试并发提交时，关键区串行化 + 乐观版本号冲突检测，下游只保留
   一份带最终执行次数的重试记录；同一请求第二次提交只记一次，按成功回执返回。
6. 数据集切换版本只改变新记录的版本戳，既有输入输出字段保持不变。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.store import store

FAILURE_TABLE = "retry_failure_batch"
EXECUTION_TABLE = "retry_execution"
ASSIGNMENT_TABLE = "retry_assignment"

# 执行数据集状态机
STATUS_WAITING = "待重试"
STATUS_RUNNING = "重试中"
STATUS_PARTIAL = "部分成功待补排"
STATUS_SUCCESS = "成功"
STATUS_EXHAUSTED = "已达上限"
DATASET_STATUSES = (STATUS_WAITING, STATUS_RUNNING, STATUS_PARTIAL, STATUS_SUCCESS, STATUS_EXHAUSTED)

# 分配清单状态（跨业务面爬虫引擎侧的投影状态）
ASSIGN_WAITING = "待分配"
ASSIGN_RUNNING = "已分配"
ASSIGN_CLOSED_OK = "成功关闭"
ASSIGN_CLOSED_LIMIT = "达上限关闭"

SOURCE_MANUAL = "人工重试"
SOURCE_SCHEDULED = "定时重试"
RETRY_SOURCES = (SOURCE_MANUAL, SOURCE_SCHEDULED)

SUPPORTED_VERSIONS = ("v1", "v2")
DEFAULT_VERSION = "v1"

# 退避类型
BACKOFF_FIXED = "固定退避"
BACKOFF_EXPONENTIAL = "指数退避"

_boot_lock = threading.Lock()
_booted = False

# 数据集当前版本保存在进程内（内存库没有元数据表），切换只影响新记录的版本戳。
_dataset_version = DEFAULT_VERSION


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.replace(microsecond=0).isoformat()


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class RetryConflictError(Exception):
    """人工/定时并发提交出现乐观版本号冲突（HTTP 409）。"""


class RetryLimitError(Exception):
    """批次已达到最大重试次数，不能再开新一轮（HTTP 409）。"""


class RetryValidationError(Exception):
    """入参缺失或不合法（HTTP 400）。"""


class RetryExecutionService:
    """失败批次 → 执行数据集 → 分配清单 的全链路领域服务。"""

    # ---- 演示数据 -------------------------------------------------------

    def ensure_demo_data(self) -> None:
        """幂等灌入演示失败批次；重启或重复调用不会产生重复记录。"""
        global _booted
        with _boot_lock:
            if _booted:
                return
            _booted = True
        if store.dataset_rows(FAILURE_TABLE):
            return

        demos = [
            {
                "采集任务编号": "CRAW-0001",
                "幂等标识": "IDEM-CRAW0001-B20261110-01",
                "失败批次编号": "FAIL-0001",
                "最大重试": 3,
                "退避类型": BACKOFF_EXPONENTIAL,
                "退避系数": 10,
                "失败条目": [
                    {"条目编号": "ITEM-1", "原始URL": "https://example.test/a/1", "失败原因": "HTTP 503"},
                    {"条目编号": "ITEM-2", "原始URL": "https://example.test/a/2", "失败原因": "读取超时"},
                ],
            },
            {
                "采集任务编号": "CRAW-0002",
                "幂等标识": "IDEM-CRAW0002-B20261111-02",
                "失败批次编号": "FAIL-0002",
                "最大重试": 2,
                "退避类型": BACKOFF_FIXED,
                "退避系数": 30,
                "失败条目": [
                    {"条目编号": "ITEM-1", "原始URL": "https://example.test/b/1", "失败原因": "代理连接拒绝"},
                    {"条目编号": "ITEM-2", "原始URL": "https://example.test/b/2", "失败原因": "HTTP 429"},
                    {"条目编号": "ITEM-3", "原始URL": "https://example.test/b/3", "失败原因": "HTTP 429"},
                ],
            },
            {
                "采集任务编号": "CRAW-0003",
                "幂等标识": "IDEM-CRAW0003-B20261112-03",
                "失败批次编号": "FAIL-0003",
                "最大重试": 1,
                "退避类型": BACKOFF_FIXED,
                "退避系数": 60,
                "失败条目": [
                    {"条目编号": "ITEM-1", "原始URL": "https://example.test/c/1", "失败原因": "DNS 解析失败"},
                ],
            },
        ]
        for demo in demos:
            self.ingest_failure_event(demo)

    # ---- 退避计算 -------------------------------------------------------

    @staticmethod
    def _backoff_seconds(record: dict[str, Any], attempt: int) -> float:
        """按任务批次统一配置的退避类型/系数计算第 attempt 轮的等待秒数。"""
        base = _to_float(record.get("退避系数"), 10.0)
        if record.get("退避类型") == BACKOFF_EXPONENTIAL:
            # 指数退避：base * 2^(attempt-1)，首轮即 base。
            return base * (2 ** max(attempt - 1, 0))
        return base

    def _compute_backoff_until(self, record: dict[str, Any], attempt: int, now: datetime) -> str:
        return _iso(now + timedelta(seconds=self._backoff_seconds(record, attempt)))

    # ---- 失败事件接入（幂等） -------------------------------------------

    def ingest_failure_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        """接收失败采集事件；同一幂等标识重复到达不创建第二份执行记录。"""
        idem_key = str(payload.get("幂等标识") or "").strip()
        task_code = str(payload.get("采集任务编号") or "").strip()
        items = payload.get("失败条目") or []
        if not idem_key:
            raise RetryValidationError("缺少幂等标识，失败事件无法幂等落库")
        if not task_code:
            raise RetryValidationError("缺少采集任务编号")
        if not isinstance(items, list) or not items:
            raise RetryValidationError("失败条目至少包含一条数据")
        clean_items = [self._clean_item(item) for item in items]

        def work() -> dict[str, Any]:
            failure_rows = store.dataset_rows(FAILURE_TABLE)
            existing = next((row for row in failure_rows if row.get("幂等标识") == idem_key), None)
            if existing is not None:
                # 重复失败事件：只累加事件计数并补登新出现的条目，执行数据集保持唯一。
                existing["失败事件次数"] = int(existing.get("失败事件次数", 1)) + 1
                existing["最后失败时间"] = _iso(_now())
                known = {entry["条目编号"] for entry in existing["失败条目"]}
                for item in clean_items:
                    if item["条目编号"] not in known:
                        existing["失败条目"].append(item)
                        known.add(item["条目编号"])
                execution = self._find_execution(idem_key)
                return {"deduplicated": True, "failure_batch": existing, "execution": execution}

            max_retry = max(1, _to_int(payload.get("最大重试"), 3))
            backoff_type = str(payload.get("退避类型") or BACKOFF_FIXED)
            backoff_factor = _to_float(payload.get("退避系数"), 10.0)
            now = _now()
            failure_id = _next_id(failure_rows)
            failure_batch = {
                "id": failure_id,
                "失败批次编号": str(payload.get("失败批次编号") or f"FAIL-{failure_id:04d}"),
                "采集任务编号": task_code,
                "幂等标识": idem_key,
                "失败条目": clean_items,
                "最大重试": max_retry,
                "退避类型": backoff_type,
                "退避系数": backoff_factor,
                "首次失败时间": _iso(now),
                "最后失败时间": _iso(now),
                "失败事件次数": 1,
                "状态记录": STATUS_WAITING,
                "数据集版本": self.get_dataset_version(),
            }
            # 失败批次、幂等标识、执行数据集、分配清单同事务写入。
            failure_rows.append(failure_batch)
            execution = self._create_execution(failure_batch, now)
            assignment = self._create_assignment(failure_batch, execution, now)
            return {
                "deduplicated": False,
                "failure_batch": failure_batch,
                "execution": execution,
                "assignment": assignment,
            }

        return store.transaction(work)

    @staticmethod
    def _clean_item(raw: Any) -> dict[str, str]:
        if not isinstance(raw, dict):
            raise RetryValidationError("失败条目必须是对象列表")
        code = str(raw.get("条目编号") or "").strip()
        if not code:
            raise RetryValidationError("失败条目缺少条目编号")
        return {
            "条目编号": code,
            "原始URL": str(raw.get("原始URL") or ""),
            "失败原因": str(raw.get("失败原因") or ""),
        }

    # ---- 执行数据集与分配清单的内部构造 ---------------------------------

    def _create_execution(self, failure_batch: dict[str, Any], now: datetime) -> dict[str, Any]:
        rows = store.dataset_rows(EXECUTION_TABLE)
        # 幂等标识唯一约束：一个失败批次永远只对应一份执行数据集。
        if any(row.get("幂等标识") == failure_batch["幂等标识"] for row in rows):
            raise RetryConflictError("该幂等标识已存在重试执行记录，禁止重复创建")
        entry_id = _next_id(rows)
        item_codes = [item["条目编号"] for item in failure_batch["失败条目"]]
        record = {
            "id": entry_id,
            "执行数据集编号": f"REX-{entry_id:04d}",
            "失败批次编号": failure_batch["失败批次编号"],
            "采集任务编号": failure_batch["采集任务编号"],
            "幂等标识": failure_batch["幂等标识"],
            "最大重试": int(failure_batch["最大重试"]),
            "退避类型": failure_batch["退避类型"],
            "退避系数": failure_batch["退避系数"],
            "退避时间": self._compute_backoff_until(failure_batch, 1, now),
            "已执行次数": 0,
            "状态记录": STATUS_WAITING,
            "触发来源": "",
            "并发触发记录": [],
            "数据集版本": failure_batch.get("数据集版本", DEFAULT_VERSION),
            "条目结果": {
                code: {"条目编号": code, "状态": "待重试", "尝试次数": 0, "最后回执": "", "最后更新": ""}
                for code in item_codes
            },
            "成功回执": [],
            "待补排条目": list(item_codes),
            "已提交请求": [],
            "开放轮次": None,
            "乐观版本号": 1,
            "创建时间": _iso(now),
            "最后更新": _iso(now),
        }
        rows.append(record)
        return record

    def _create_assignment(
        self, failure_batch: dict[str, Any], execution: dict[str, Any], now: datetime
    ) -> dict[str, Any]:
        rows = store.dataset_rows(ASSIGNMENT_TABLE)
        entry_id = _next_id(rows)
        assignment = {
            "id": entry_id,
            "分配清单编号": f"RAS-{entry_id:04d}",
            "爬虫任务编号": failure_batch["采集任务编号"],
            "失败批次编号": failure_batch["失败批次编号"],
            "幂等标识": failure_batch["幂等标识"],
            "执行数据集编号": execution["执行数据集编号"],
            "数据集版本": execution["数据集版本"],
            "已执行次数": 0,
            "最大重试": execution["最大重试"],
            "退避时间": execution["退避时间"],
            "分配状态": ASSIGN_WAITING,
            "待处理条目": list(execution["待补排条目"]),
            "最后更新": _iso(now),
        }
        rows.append(assignment)
        return assignment

    def _find_execution(self, idem_key: str) -> dict[str, Any] | None:
        return next(
            (row for row in store.dataset_rows(EXECUTION_TABLE) if row.get("幂等标识") == idem_key),
            None,
        )

    def _find_execution_by_batch(self, batch_code: str) -> dict[str, Any] | None:
        return next(
            (row for row in store.dataset_rows(EXECUTION_TABLE)
             if row.get("失败批次编号") == batch_code or row.get("幂等标识") == batch_code),
            None,
        )

    def _find_assignment(self, idem_key: str) -> dict[str, Any] | None:
        return next(
            (row for row in store.dataset_rows(ASSIGNMENT_TABLE) if row.get("幂等标识") == idem_key),
            None,
        )

    # ---- 查询 -----------------------------------------------------------

    def list_failure_batches(self) -> list[dict[str, Any]]:
        self.ensure_demo_data()
        return store.dataset_rows(FAILURE_TABLE)

    def list_executions(self) -> list[dict[str, Any]]:
        self.ensure_demo_data()
        return store.dataset_rows(EXECUTION_TABLE)

    def get_execution(self, identifier: str) -> dict[str, Any] | None:
        self.ensure_demo_data()
        return self._find_execution_by_batch(identifier)

    # ---- 开轮：人工/定时重试提交（并发只保留一份记录） -------------------

    def submit_retry_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        """人工重试或定时重试入口；并发提交冲突时下游只存在一份重试记录。"""
        identifier = str(payload.get("幂等标识") or payload.get("失败批次编号") or "").strip()
        source = str(payload.get("触发来源") or "").strip()
        request_id = str(payload.get("请求幂等号") or "").strip()
        expect_version = payload.get("期望版本号")
        if not identifier:
            raise RetryValidationError("缺少幂等标识或失败批次编号")
        if source not in RETRY_SOURCES:
            raise RetryValidationError(f"触发来源仅支持：{'、'.join(RETRY_SOURCES)}")

        def work() -> dict[str, Any]:
            record = self._find_execution_by_batch(identifier)
            if record is None:
                raise RetryValidationError(f"未找到失败批次 {identifier} 对应的重试执行记录")
            self._check_optimistic_version(record, expect_version)

            # 同一请求第二次提交：只记一次，直接按成功回执口径返回既有记录。
            if request_id and request_id in record["已提交请求"]:
                return {
                    "deduplicated": True,
                    "conflict_merged": False,
                    "message": "该重试请求已提交过，按既有成功回执返回，未重复计数",
                    "execution": record,
                    "assignment": self._find_assignment(record["幂等标识"]),
                }

            if record["开放轮次"] is not None:
                # 人工与定时并发：轮次已被另一来源打开，合并来源但不再增加执行次数。
                if source not in record["并发触发记录"]:
                    record["并发触发记录"].append(source)
                if request_id:
                    record["已提交请求"].append(request_id)
                record["最后更新"] = _iso(_now())
                self._sync_assignment(record)
                return {
                    "deduplicated": False,
                    "conflict_merged": True,
                    "message": f"{source}与既有轮次并发，已合并到同一份重试记录，执行次数不重复增加",
                    "execution": record,
                    "assignment": self._find_assignment(record["幂等标识"]),
                }

            if int(record["已执行次数"]) >= int(record["最大重试"]):
                raise RetryLimitError("该失败批次已达到最大重试次数，不能再次提交")

            now = _now()
            attempt = int(record["已执行次数"]) + 1
            record["已执行次数"] = attempt
            record["开放轮次"] = {
                "轮次": attempt,
                "触发来源": source,
                "请求幂等号": request_id,
                "开启时间": _iso(now),
            }
            record["触发来源"] = source
            if source not in record["并发触发记录"]:
                record["并发触发记录"].append(source)
            if request_id:
                record["已提交请求"].append(request_id)
            record["状态记录"] = STATUS_RUNNING
            record["退避时间"] = self._compute_backoff_until(record, attempt + 1, now)
            record["乐观版本号"] = int(record["乐观版本号"]) + 1
            record["最后更新"] = _iso(now)
            for code in record["待补排条目"]:
                item_state = record["条目结果"][code]
                item_state["状态"] = "重试中"
                item_state["最后更新"] = _iso(now)
            self._sync_assignment(record)
            return {
                "deduplicated": False,
                "conflict_merged": False,
                "message": f"第 {attempt} 轮重试已按整组提交，等待批量回执",
                "execution": record,
                "assignment": self._find_assignment(record["幂等标识"]),
            }

        return store.transaction(work)

    # ---- 整组执行 + 批内逐条回执（部分失败只补排不覆盖） -----------------

    def submit_execution_receipts(self, payload: dict[str, Any]) -> dict[str, Any]:
        """批量入口直接提交整组；批内每条数据独立返回结果。

        同一请求幂等号重复提交只生效一次；部分失败保留全部历史成功回执，
        失败项进入补排，绝不整批覆盖既有执行结果。
        """
        identifier = str(payload.get("幂等标识") or payload.get("失败批次编号") or "").strip()
        request_id = str(payload.get("请求幂等号") or "").strip()
        raw_results = payload.get("条目结果")
        expect_version = payload.get("期望版本号")
        if not identifier:
            raise RetryValidationError("缺少幂等标识或失败批次编号")
        if not isinstance(raw_results, list) or not raw_results:
            raise RetryValidationError("条目结果必须为非空列表，整组逐条提交")

        def work() -> dict[str, Any]:
            record = self._find_execution_by_batch(identifier)
            if record is None:
                raise RetryValidationError(f"未找到失败批次 {identifier} 对应的重试执行记录")
            self._check_optimistic_version(record, expect_version)

            # 同一请求第二次提交：只记一次，按成功回执返回既有数据集。
            if request_id and request_id in record["已提交请求"]:
                return {
                    "deduplicated": True,
                    "message": "该批量回执已提交过，按既有成功回执返回，未重复执行",
                    "execution": record,
                    "assignment": self._find_assignment(record["幂等标识"]),
                }

            now = _now()
            if record["开放轮次"] is None:
                # 未显式开轮时，批量入口自身开一轮（仍受最大重试约束）。
                if int(record["已执行次数"]) >= int(record["最大重试"]):
                    raise RetryLimitError("该失败批次已达到最大重试次数，不能再次执行")
                attempt = int(record["已执行次数"]) + 1
                record["已执行次数"] = attempt
                record["开放轮次"] = {
                    "轮次": attempt,
                    "触发来源": record.get("触发来源") or SOURCE_MANUAL,
                    "请求幂等号": request_id,
                    "开启时间": _iso(now),
                }
                record["状态记录"] = STATUS_RUNNING
            round_no = int(record["开放轮次"]["轮次"])

            indexed: dict[str, dict[str, Any]] = {}
            for raw in raw_results:
                if not isinstance(raw, dict) or not str(raw.get("条目编号") or "").strip():
                    raise RetryValidationError("每条条目结果必须包含条目编号")
                indexed[str(raw["条目编号"]).strip()] = raw

            # 只允许提交当前仍在补排队列里的条目，防止重放已成功项。
            unknown = [code for code in indexed if code not in record["条目结果"]]
            if unknown:
                raise RetryValidationError(f"条目不属于该失败批次：{'、'.join(unknown)}")
            stale = [code for code in indexed if code not in record["待补排条目"]]
            if stale:
                raise RetryValidationError(f"条目已成功，禁止覆盖既有回执：{'、'.join(stale)}")
            missing = [code for code in record["待补排条目"] if code not in indexed]
            if missing:
                raise RetryValidationError(
                    f"整组执行要求批内每条数据独立返回结果，缺少：{'、'.join(missing)}"
                )

            succeeded: list[str] = []
            failed: list[str] = []
            for code in record["待补排条目"]:
                raw = indexed[code]
                ok = bool(raw.get("是否成功"))
                note = str(raw.get("回执") or raw.get("回执内容") or "")
                item_state = record["条目结果"][code]
                item_state["尝试次数"] = int(item_state.get("尝试次数", 0)) + 1
                item_state["最后更新"] = _iso(now)
                if ok:
                    # 成功回执只追加，不覆盖历史。
                    receipt = {
                        "回执编号": f"RCPT-{record['执行数据集编号']}-R{round_no}-{code}",
                        "条目编号": code,
                        "执行轮次": round_no,
                        "状态": "成功",
                        "回执内容": note or f"第 {round_no} 轮重试成功",
                        "时间": _iso(now),
                    }
                    record["成功回执"].append(receipt)
                    item_state["状态"] = "成功"
                    item_state["最后回执"] = receipt["回执内容"]
                    succeeded.append(code)
                else:
                    item_state["状态"] = "失败"
                    item_state["最后回执"] = note or f"第 {round_no} 轮重试失败"
                    failed.append(code)

            # 失败项补排：从待补排队列移除成功项，失败项保留，绝不整批覆盖。
            record["待补排条目"] = [code for code in record["待补排条目"] if code in failed]
            if request_id:
                record["已提交请求"].append(request_id)
            record["乐观版本号"] = int(record["乐观版本号"]) + 1
            record["最后更新"] = _iso(now)
            record["开放轮次"] = None

            if not record["待补排条目"]:
                record["状态记录"] = STATUS_SUCCESS
            elif int(record["已执行次数"]) >= int(record["最大重试"]):
                record["状态记录"] = STATUS_EXHAUSTED
            else:
                record["状态记录"] = STATUS_PARTIAL
                record["退避时间"] = self._compute_backoff_until(
                    record, int(record["已执行次数"]) + 1, now
                )

            self._sync_assignment(record)
            return {
                "deduplicated": False,
                "round": round_no,
                "succeeded": succeeded,
                "requeued": failed,
                "message": f"第 {round_no} 轮整组执行完成：成功 {len(succeeded)} 条，补排 {len(failed)} 条",
                "execution": record,
                "assignment": self._find_assignment(record["幂等标识"]),
            }

        return store.transaction(work)

    # ---- 分配清单：爬虫引擎跨业务面消费 ---------------------------------

    def _sync_assignment(self, record: dict[str, Any]) -> None:
        """执行数据集状态落到分配清单；与数据集写入处于同一事务。"""
        assignment = self._find_assignment(record["幂等标识"])
        if assignment is None:
            raise RetryConflictError("分配清单缺失，事务将整体回滚")
        now = _iso(_now())
        assignment["已执行次数"] = record["已执行次数"]
        assignment["退避时间"] = record["退避时间"]
        assignment["数据集版本"] = record["数据集版本"]
        assignment["待处理条目"] = list(record["待补排条目"])
        assignment["最后更新"] = now
        if record["状态记录"] == STATUS_SUCCESS:
            assignment["分配状态"] = ASSIGN_CLOSED_OK
        elif record["状态记录"] == STATUS_EXHAUSTED:
            assignment["分配状态"] = ASSIGN_CLOSED_LIMIT
        elif record["状态记录"] == STATUS_RUNNING:
            assignment["分配状态"] = ASSIGN_RUNNING
        else:
            assignment["分配状态"] = ASSIGN_WAITING

    def build_assignment_list(self, *, task_code: str | None = None) -> list[dict[str, Any]]:
        """爬虫引擎消费口：只包含尚未成功且未达到上限的批次。"""
        self.ensure_demo_data()
        result = []
        for assignment in store.dataset_rows(ASSIGNMENT_TABLE):
            if assignment["分配状态"] not in (ASSIGN_WAITING, ASSIGN_RUNNING):
                continue
            if int(assignment["已执行次数"]) >= int(assignment["最大重试"]):
                continue
            if not assignment["待处理条目"]:
                continue
            if task_code and assignment["爬虫任务编号"] != task_code:
                continue
            result.append(assignment)
        return result

    # ---- 版本切换：字段保持稳定 -----------------------------------------

    def get_dataset_version(self) -> str:
        return _dataset_version

    def switch_dataset_version(self, version: str) -> dict[str, Any]:
        """切换执行数据集版本；既有记录的输入输出字段一个都不改。"""
        version = str(version or "").strip()
        if version not in SUPPORTED_VERSIONS:
            raise RetryValidationError(f"仅支持版本：{'、'.join(SUPPORTED_VERSIONS)}")

        global _dataset_version
        previous = _dataset_version

        def work() -> dict[str, Any]:
            global _dataset_version
            _dataset_version = version
            # 只改新记录的默认版本戳；既有执行数据集与分配清单字段原样保留。
            for record in store.dataset_rows(EXECUTION_TABLE):
                if record.get("数据集版本") is None:
                    record["数据集版本"] = version
            return {
                "previous_version": previous,
                "active_version": version,
                "kept_fields": self.dataset_field_contract(),
                "message": "版本已切换，既有输入输出字段保持不变",
            }

        try:
            return store.transaction(work)
        except Exception:
            # 数据集回滚时，进程内版本标记也要一起回滚，保持同事务语义。
            _dataset_version = previous
            raise

    @staticmethod
    def dataset_field_contract() -> list[str]:
        """v1/v2 共用的稳定字段契约，版本切换不允许破坏。"""
        return [
            "执行数据集编号", "失败批次编号", "采集任务编号", "幂等标识",
            "最大重试", "退避类型", "退避系数", "退避时间", "已执行次数",
            "状态记录", "触发来源", "条目结果", "成功回执", "待补排条目",
        ]

    # ---- 并发工具 -------------------------------------------------------

    @staticmethod
    def _check_optimistic_version(record: dict[str, Any], expect_version: Any) -> None:
        if expect_version is None:
            return
        if int(expect_version) != int(record["乐观版本号"]):
            raise RetryConflictError(
                f"并发提交冲突：期望版本号 {expect_version}，当前版本号 {record['乐观版本号']}"
            )

    def run_concurrent(self, *calls: Callable[[], dict[str, Any]]) -> list[dict[str, Any]]:
        """串行化执行多个并发入口（人工/定时同时提交），供接口层与演示使用。"""
        results: list[dict[str, Any]] = []

        def runner(call: Callable[[], dict[str, Any]]) -> None:
            try:
                results.append(call())
            except (RetryConflictError, RetryLimitError, RetryValidationError) as exc:
                results.append({"error": type(exc).__name__, "message": str(exc)})

        threads = []
        for call in calls:
            thread = threading.Thread(target=runner, args=(call,))
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return results


service = RetryExecutionService()

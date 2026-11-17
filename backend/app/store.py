"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

失败重试扩展（v2）在业务模块表之外新增了三类“数据集”，它们和模块表隔离存放，
不进入运营概览的模块计数：

- retry_failure_batch：失败采集批次（失败事件的幂等落点）
- retry_execution：重试执行数据集（每个失败批次仅一份，带最终执行次数）
- retry_assignment：重试任务分配清单（跨业务面爬虫引擎消费的状态投影）

数据集写入统一走 ``transaction``：进入时做深拷贝快照，业务块抛错即整体回滚，
保证“重试执行数据集与重试任务分配清单同事务、失败一起回滚”。
"""
from __future__ import annotations

import copy
import threading
from typing import Any, Callable, TypeVar

from app.seed import SEED_ROWS

T = TypeVar("T")

# 失败重试扩展使用的数据集名称，和模块表（SEED_ROWS 的 key）隔离。
RETRY_DATASETS = ("retry_failure_batch", "retry_execution", "retry_assignment")


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 扩展数据集：独立命名空间，不参与 module_names/overview。
        for name in RETRY_DATASETS:
            self._tables[name] = []
        # 跨线程的人工/定时并发提交需要同一把锁串行化关键区。
        self._lock = threading.RLock()

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables if name not in RETRY_DATASETS)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def dataset_rows(self, name: str) -> list[dict[str, Any]]:
        """读取扩展数据集；未登记的名称直接报错，避免拼写错误静默建表。"""
        if name not in RETRY_DATASETS:
            raise KeyError(f"未知的重试数据集：{name}")
        return self._tables.setdefault(name, [])

    def transaction(self, work: Callable[[], T]) -> T:
        """在锁内执行业务块，异常时按快照整体回滚全部数据集与模块表。

        标准库内存库没有数据库事务，这里用深拷贝快照模拟 commit/rollback：
        失败重试要求“重试执行数据集和重试任务分配清单放进同一事务，
        重试执行数据集失败时一起回滚”，业务块里任何一步抛错都会恢复现场。
        """
        with self._lock:
            snapshot = copy.deepcopy(self._tables)
            try:
                return work()
            except Exception:
                self._tables.clear()
                self._tables.update(snapshot)
                raise

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()

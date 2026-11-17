"""失败重试接口：维护重试策略，覆盖提交校验、启用配置、停用配置等动作。

v2 扩展（重试执行数据集）接口都挂在 /api/retry 下：

- POST   /failure-events            失败采集事件接入（幂等）
- GET    /failure-batches           失败批次核对
- GET    /executions                重试执行数据集列表
- GET    /executions/{标识}         单份执行数据集明细
- POST   /retry-requests            人工/定时重试提交（并发只保留一份记录）
- POST   /execution-receipts        整组执行、批内逐条回执（部分失败只补排）
- GET    /assignment-list           重试任务分配清单（也经爬虫引擎路由暴露）
- POST   /dataset-version/switch    执行数据集版本切换（字段保持稳定）
- GET    /dataset-version           当前数据集版本与字段契约

注意：所有静态 GET 路径必须声明在 /{entry_id} 之前，否则会被参数路由截走。
"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, PageResult
from app.services.retry import RetryService
from app.services.retry_execution import (
    RetryConflictError,
    RetryExecutionService,
    RetryLimitError,
    RetryValidationError,
)

router = APIRouter(prefix="/api/retry", tags=["失败重试"])

service = RetryService()
execution_service = RetryExecutionService()

LIST_FIELDS = ["策略编号", "适用任务", "最大重试", "退避类型", "退避系数", "幂等标识", "状态记录", "最后更新"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


def _domain_error(exc: Exception) -> HTTPException:
    """把领域异常映射成 HTTP 状态码：参数 400，并发/上限 409。"""
    if isinstance(exc, RetryValidationError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, (RetryConflictError, RetryLimitError)):
        return HTTPException(status_code=409, detail=str(exc))
    raise exc


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


# --------------------------------------------------------------------- #
# v2：幂等重试批次与重试执行数据集（静态路径必须排在 /{entry_id} 之前）
# --------------------------------------------------------------------- #

@router.post("/failure-events")
def ingest_failure_event(payload: dict[str, object]) -> dict:
    """接收失败采集事件；同一幂等标识重复到达不会创建第二份执行记录。"""
    try:
        result = execution_service.ingest_failure_event(payload)
    except (RetryValidationError, RetryConflictError, RetryLimitError) as exc:
        raise _domain_error(exc)
    result["ok"] = True
    return result


@router.get("/failure-batches")
def list_failure_batches() -> dict:
    """核对失败批次：批次编号、幂等标识、退避与失败条目。"""
    return {"items": execution_service.list_failure_batches()}


@router.get("/executions")
def list_executions() -> dict:
    """重试执行数据集：退避、次数、状态记录按任务批次统一管理。"""
    return {"items": execution_service.list_executions()}


@router.get("/executions/{identifier}")
def get_execution(identifier: str) -> dict:
    """按失败批次编号或幂等标识读取单份执行数据集（含逐条回执）。"""
    record = execution_service.get_execution(identifier)
    if record is None:
        raise HTTPException(status_code=404, detail=f"失败批次 {identifier} 暂无重试执行记录")
    return record


@router.post("/retry-requests")
def submit_retry_request(payload: dict[str, object]) -> dict:
    """人工重试 / 定时重试提交；并发冲突时下游只保留一份带最终执行次数的记录。"""
    try:
        return execution_service.submit_retry_request(payload)
    except (RetryValidationError, RetryConflictError, RetryLimitError) as exc:
        raise _domain_error(exc)


@router.post("/execution-receipts")
def submit_execution_receipts(payload: dict[str, object]) -> dict:
    """批量入口直接提交整组，批内每条数据独立返回结果；重复请求只记一次。"""
    try:
        return execution_service.submit_execution_receipts(payload)
    except (RetryValidationError, RetryConflictError, RetryLimitError) as exc:
        raise _domain_error(exc)


@router.get("/assignment-list")
def get_assignment_list(
    task_code: str | None = Query(default=None, alias="爬虫任务编号", description="可选：按爬虫任务过滤"),
) -> dict:
    """重试任务分配清单：只含尚未成功且未达到上限的批次。"""
    return {"items": execution_service.build_assignment_list(task_code=task_code)}


@router.get("/dataset-version")
def get_dataset_version() -> dict:
    """当前执行数据集版本与稳定字段契约。"""
    return {
        "active_version": execution_service.get_dataset_version(),
        "supported_versions": ["v1", "v2"],
        "field_contract": execution_service.dataset_field_contract(),
    }


@router.post("/dataset-version/switch")
def switch_dataset_version(payload: dict[str, object]) -> dict:
    """切换执行数据集版本；既有输入输出字段保持不变。"""
    try:
        return execution_service.switch_dataset_version(str(payload.get("version") or ""))
    except RetryValidationError as exc:
        raise _domain_error(exc)


# --------------------------------------------------------------------- #
# 原有重试策略 CRUD（参数路由放最后，避免截走静态路径）
# --------------------------------------------------------------------- #

@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、策略编号关键字与状态过滤失败重试列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="retry.csv"'})


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条重试策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"重试策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条重试策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="重试策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条重试策略执行提交校验、启用配置、停用配置；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

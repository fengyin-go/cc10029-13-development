# 智能爬虫采集平台

面向网页爬取、API接入、增量抓取、反爬对抗与采集质量校验的数据采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集目标 | `targetsite` | 目标站点 | 站点编号、站点名称、入口URL |
| 抽取规则 | `selector` | 选择器规则 | 规则编号、归属站点、列表选择器 |
| 爬虫引擎 | `crawler` | 爬虫任务 | 任务编号、任务名称、关联站点 |
| 代理池 | `proxy` | 代理节点 | 节点编号、IP地址、端口号 |
| 反爬对抗 | `antiblock` | 反爬策略 | 策略编号、策略名称、触发条件 |
| 定时调度 | `schedule` | 调度计划 | 计划编号、关联任务、Cron表达式 |
| 数据提取 | `extractor` | 提取模板 | 模板编号、模板名称、输入URL |
| 采集校验 | `validate` | 校验规则 | 规则编号、校验字段、期望格式 |
| 失败重试 | `retry` | 重试策略 | 策略编号、适用任务、最大重试 |
| 存储落地 | `storage` | 存储配置 | 配置编号、存储类型、连接信息 |
| 重复去重 | `dedup` | 去重策略 | 策略编号、业务键列、去重算法 |
| 质量报告 | `quality` | 质量指标 | 指标编号、指标名称、统计口径 |
| 运行监控 | `monitor` | 监控大盘 | 大盘编号、关联任务、采集速率 |
| 告警中心 | `alert` | 告警规则 | 规则编号、监控对象、告警条件 |
| 采集API | `apigate` | API配置 | 配置编号、端点路径、请求方法 |
| 历史归档 | `archive` | 归档策略 | 策略编号、数据来源、归档周期 |
| 模板市场 | `template` | 采集模板 | 模板编号、模板名称、适用站点 |
| 采集流程 | `flowline` | 流程编排 | 流程编号、流程名称、步骤序列 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 失败重试：幂等重试批次与重试执行数据集

失败重试模块在原重试策略 CRUD 之外，新增了按任务批次统一管理的重试执行数据集，
领域规则集中在 `backend/app/services/retry_execution.py`，数据集与模块表隔离存放于
内存仓库（`retry_failure_batch` / `retry_execution` / `retry_assignment`）。

| 能力 | 接口 |
| --- | --- |
| 失败事件接入（幂等标识去重，重复事件不建第二份执行记录） | `POST /api/retry/failure-events` |
| 失败批次 / 执行数据集核对 | `GET /api/retry/failure-batches`、`GET /api/retry/executions` |
| 人工/定时重试提交（并发只保留一份带最终执行次数的记录） | `POST /api/retry/retry-requests` |
| 整组执行、批内逐条回执（部分失败只补排不覆盖） | `POST /api/retry/execution-receipts` |
| 重试任务分配清单（只含尚未成功且未达上限的批次） | `GET /api/retry/assignment-list`、`GET /api/crawler/retry-assignments` |
| 数据集版本切换（既有输入输出字段不变） | `GET/POST /api/retry/dataset-version[/switch]` |

关键约束：

- 失败批次、幂等标识、执行数据集与分配清单在同一事务写入；任一步失败按快照整体回滚
  （见 `Store.transaction`）。
- 同一请求幂等号第二次提交只记一次，按既有成功回执返回。
- 人工重试与定时重试并发提交由锁 + 乐观版本号（`期望版本号`，冲突返回 409）串行化，
  下游只存在一份重试记录。
- 退避时间、重试次数、状态记录全部挂在执行数据集记录上，状态经分配清单投影到跨业务面
  爬虫引擎；前端「失败重试」页底部工作台可核对失败批次、次数、退避、回执与并发留痕。

领域：数据工程
子方向：数据采集、爬虫与RPA

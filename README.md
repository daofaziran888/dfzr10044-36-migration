# 湖表工坊 · 湖表分区演化与小文件归并平台

面向湖表目录登记、表结构演进、分区规划与演化、小文件归并、快照与时间旅行、入湖水位推进、压实调度、统计画像、隔离区处置、消费视图发布与存储分层的湖表治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
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

健康检查：`curl http://127.0.0.1:8001/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5174/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8001`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 湖表目录 | `lake_catalog` | 湖表条目 | 表编号、表名全称、所属库域 |
| 结构演进 | `table_schema` | 结构版本 | 版本编号、所属湖表、列清单 |
| 分区规划 | `partition_plan` | 分区方案 | 方案编号、所属湖表、分区键 |
| 分区演化 | `partition_evolve` | 演化记录 | 演化编号、所属湖表、原分区键 |
| 小文件归并 | `small_file_merge` | 归并任务 | 归并编号、所属湖表、目标分区 |
| 快照管理 | `snapshot_mgr` | 湖表快照 | 快照编号、所属湖表、快照标识 |
| 时间旅行 | `time_travel` | 回溯查询 | 查询编号、所属湖表、目标快照 |
| 入湖水位 | `ingest_watermark` | 水位记录 | 水位编号、所属湖表、水位字段 |
| 入湖批次 | `ingest_batch` | 入湖批次 | 批次编号、所属湖表、来源系统 |
| 写入作业 | `write_job` | 写入作业 | 作业编号、所属湖表、写入模式 |
| 压实作业 | `compact_job` | 压实作业 | 压实编号、所属湖表、触发条件 |
| 过期策略 | `expire_policy` | 过期策略 | 策略编号、所属湖表、保留快照数 |
| 统计画像 | `stats_profile` | 统计画像 | 画像编号、所属湖表、统计粒度 |
| 列统计 | `column_stat` | 列统计 | 统计编号、所属湖表、列名称 |
| 入湖质量规则 | `quality_rule` | 质量规则 | 规则编号、所属湖表、校验列 |
| 隔离区 | `quarantine_zone` | 隔离记录 | 隔离编号、所属湖表、来源批次 |
| 重建补数 | `rebuild_backfill` | 补数任务 | 补数编号、所属湖表、补数窗口 |
| 存储分层 | `storage_tier` | 分层规则 | 分层编号、所属湖表、热层判定 |
| 保留窗口 | `retention_window` | 保留窗口 | 窗口编号、所属湖表、保留粒度 |
| 消费视图 | `publish_view` | 消费视图 | 视图编号、所属湖表、视图定义 |
| 资源额度计量 | `quota_meter` | 额度读数 | 读数编号、所属湖表、额度类型 |
| 湖表审计 | `lake_audit` | 审计条目 | 条目编号、所属湖表、操作类型 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 隔离区存量迁移与回炉入口切换

隔离区支持把按旧批次+规则字段保存的存量记录迁移成逐行可回查的处置清单：

- `backend/app/pipeline/quarantine_migration.py` 迁移流水线（`chain_quarantine_disposition_migration`）：
  旧批扫描 → 逐行迁移 → 两路核对；按行落检查点，写到一半停机可断点续跑，重复执行幂等。
- 新旧回炉入口共用同一行级确认日志：同一行的有效确认合一，后续入湖不会多出一行。
- 批号按湖表核对：另表同号的逐行记录继续归属原表。
- 缺少可信回炉凭证的记录进入待核并保留旧内容；还原旧入口后从原隔离清单仍能追到同一行。
- 行级接口挂在 `/api/quarantine/`（迁移、恢复、处置清单、领取、回炉重试、旧入口视图、两路核对、凭证核验），
  前端隔离批次详情页：`/quarantine_zone/batch/:lakeTable/:batchId`。

领域：数据工程
子方向：数仓与数据湖

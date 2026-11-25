"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import quarantine, resource

# 隔离区行级接口（/api/quarantine/...）与通用资源路由（/api/{module}）路径不冲突
ROUTERS = [quarantine, resource]

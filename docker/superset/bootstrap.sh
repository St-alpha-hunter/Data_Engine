#!/bin/bash
# 启动时：升级元数据库 → 确保管理员存在 → 初始化角色权限 → 启动网页服务（都可以重复执行）
set -e
superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname User \
    --email admin@localhost --password "${SUPERSET_ADMIN_PASSWORD}" || true
superset init
exec /app/docker/entrypoints/run-server.sh

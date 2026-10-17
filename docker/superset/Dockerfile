# 项目用的 Superset 镜像：官方 6.1.0 + 数据库驱动（官方镜像不带 PostgreSQL / ClickHouse 驱动）
FROM apache/superset:6.1.0
USER root
# 装进 Superset 自己的虚拟环境 /app/.venv；用清华源（国内直连 PyPI 慢）
RUN uv pip install --python /app/.venv/bin/python --no-cache --index-url https://pypi.tuna.tsinghua.edu.cn/simple \
        "psycopg2-binary==2.9.10" "clickhouse-connect==0.15.1"
USER superset

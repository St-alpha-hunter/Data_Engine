docker compose up -d        # 启动
docker compose stop         # 停止
docker compose logs -f      # 看日志
docker exec -it clickhouse clickhouse-client --user data_engine --password <.env里的密码>   # 命令行进去写 SQL

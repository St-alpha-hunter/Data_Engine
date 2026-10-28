### 进程命令速查

# 1. 找进程：哪些 python 在跑、跑的是什么命令
Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Select ProcessId, CommandLine

# 2. 看进度：实时跟踪日志最后几行（Ctrl+C 退出，不会影响进程）
Get-Content E:\data_process\data\logs\backfill_20261013.log -Tail 5 -Wait

# 3. 停止进程（把 1592 换成第 1 步查到的 ProcessId）
Stop-Process -Id 1592


# link google
gcloud.cmd compute ssh instance-20260902-124649 --zone=europe-west2-a --project=project-311b5e7b-05a8-4153-bd2 --tunnel-through-iap -- -N -L 127.0.0.1:18088:127.0.0.1:18088

# 小规格云服务器在执行大批量 Parquet → ClickHouse 导入时，可能遇到峰值内存不足。
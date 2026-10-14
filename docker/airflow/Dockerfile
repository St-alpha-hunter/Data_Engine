# 项目用的 Airflow 镜像：官方 3.3.2（Python 3.10，和本机环境一致）+ 项目依赖
# 构建：docker compose build airflow-init（其他 airflow 服务共用这个镜像）
FROM apache/airflow:3.3.2-python3.10

COPY requirements.txt /tmp/requirements.txt
# 同时写上 apache-airflow==3.3.2：防止装依赖时把 Airflow 本身升级掉；用清华源（国内直连 PyPI 慢）
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
        "apache-airflow==3.3.2" -r /tmp/requirements.txt

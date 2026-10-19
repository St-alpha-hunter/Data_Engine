# Spark 3.5.8（Java 17、Python 3）+ Delta Lake 3.3.2 + Kafka 连接器 + PostgreSQL JDBC
# 构建：docker compose build spark
# jar 在构建时下载好（用 python 下载：基础镜像不一定有 wget / curl）放进 /opt/spark/jars，运行时不再联网（--packages 要连 Maven 中央仓库，国内慢且常失败）
# 用阿里云 Maven 镜像；版本要配套：Delta 3.3.x 对应 Spark 3.5，Kafka 连接器和 Spark 同版本，kafka-clients 用连接器声明的 3.4.1
FROM apache/spark:3.5.8-java17-python3

USER root
ARG MAVEN=https://maven.aliyun.com/repository/public
RUN cd /opt/spark/jars && for p in \
        io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
        io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
        org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar \
        org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar \
        org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
        org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
        org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar ; do \
      python3 -c "import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])" "$MAVEN/$p" "$(basename $p)" || exit 1; \
    done
# delta-spark 的 Python 包只是一层封装（DeltaTable.forPath 等），--no-deps：pyspark 镜像里已经有了
RUN pip install --no-cache-dir --no-deps -i https://pypi.tuna.tsinghua.edu.cn/simple delta-spark==3.3.2
USER spark

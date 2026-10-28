                  Alpha-Deputy Data Platform
                  ==========================

                    LOCAL ENVIRONMENT
                   (Windows / Docker)
                           |
                           |
                  Apache Airflow
                  (Orchestration)
                           |
              +------------+------------+
              |                         |
              v                         v
         FMP API                    Kafka
              |                         |
              v                         v
          ETL / ELT                   Spark
              |                         |
       +------+-------+                 |
       |              |                 |
       v              v                 v
   PostgreSQL     ClickHouse       Delta Lake
   (Metadata)     (Raw/Gold)      (Bronze/Silver)
       |              |                 |
       +--------------+-----------------+
                      |
                      v
               Export / Backup
                      |
                      v
         =========================
             GOOGLE CLOUD
         =========================
                      |
                      v
              Google Cloud Storage
                      |
                      |
             +--------+---------+
             |                  |
             v                  v
         Backups             Data Lake
                              Mirror
                                |
                                v
                         Compute Engine
                          (e2-medium)
                                |
                         Docker Compose
                                |
                    +-----------+----------+
                    |                      |
                    v                      v
                ClickHouse              Superset
                (Analytics)            (Dashboard)
                    ^                      |
                    |                      |
                    +----------------------+
                                |
                                v
                         Web Browser
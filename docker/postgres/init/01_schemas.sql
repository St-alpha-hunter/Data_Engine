-- 首次启动容器时自动执行（数据卷为空时才会跑）
CREATE SCHEMA IF NOT EXISTS meta;   -- 数据治理工具表：ingestion / quality_issues / quarantine / corrections

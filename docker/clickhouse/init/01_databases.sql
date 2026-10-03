-- 首次启动容器时自动执行（数据卷为空时才会跑）
CREATE DATABASE IF NOT EXISTS raw;      -- FMP 原始数据，原样落库
CREATE DATABASE IF NOT EXISTS golden;   -- 清洗校验通过的数据

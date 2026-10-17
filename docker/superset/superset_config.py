"""
Superset 配置（挂到容器的 /app/pythonpath/superset_config.py）
元数据库：postgres 容器里的 superset 库；密钥从 .env 读
"""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://superset:{os.environ['SUPERSET_DB_PASSWORD']}@postgres:5432/superset"

BABEL_DEFAULT_LOCALE = "zh"                    # 界面默认中文
LANGUAGES = {"zh": {"flag": "cn", "name": "简体中文"}, "en": {"flag": "us", "name": "English"}}

FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
ROW_LIMIT = 50000
SQLLAB_TIMEOUT = 300

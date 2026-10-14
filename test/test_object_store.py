from pathlib import Path

import pandas as pd

from config.paths import BASE_DIR
from db.object_store import IntermediateStore

"""

中间态存储的路径可移植：Windows 本机写的、Docker 容器（Airflow）里读，反过来也一样

"""


def test_location_inside_project_is_relative():
    loc = IntermediateStore().location("price_volume", "b1")
    assert not Path(loc).is_absolute() and loc.startswith("data/") and "\\" not in loc


def test_resolve_relative_and_foreign_absolute_paths():
    assert IntermediateStore.resolve("data/x/y.parquet") == str(BASE_DIR / "data" / "x" / "y.parquet")
    # 别的系统上记下的绝对路径（这台机器上不存在）：按 data/ 之后的部分换算到当前项目目录
    assert IntermediateStore.resolve(r"Z:\old_place\data_process\data\volume_data_1y\b.parquet") == \
        str(BASE_DIR / "data" / "volume_data_1y" / "b.parquet")
    assert IntermediateStore.resolve("s3://bucket/x.parquet") == "s3://bucket/x.parquet"


def test_write_read_roundtrip_outside_project(tmp_path):
    store = IntermediateStore(str(tmp_path))
    df = pd.DataFrame({"a": [1, 2]})
    loc = store.write(df, "ep", "b1")
    assert Path(loc).is_absolute()                     # 项目目录外：记绝对路径
    assert store.read(loc)["a"].tolist() == [1, 2]

"""
按接口名查找清洗类

清洗类在 clean/ 下各自的文件里定义，写上 endpoint_name 就会自动注册到 CleanBasic.registry。
这里负责把 clean/ 下所有 clean_*.py 导入一遍（导入时才会触发注册），再按接口名查找。

    from clean.registry import get_cleaner
    cleaner_cls = get_cleaner("price_volume")      # -> PriceVolume
"""
import importlib
from pathlib import Path

from clean.base_clean import CleanBasic

_loaded = False


def load_all() -> dict[str, type]:
    global _loaded
    if not _loaded:
        for path in sorted(Path(__file__).parent.glob("clean_*.py")):
            importlib.import_module(f"clean.{path.stem}")
        _loaded = True
    return CleanBasic.registry


def get_cleaner(endpoint: str) -> type[CleanBasic]:
    registry = load_all()
    if endpoint not in registry:
        raise KeyError(f"没有 endpoint={endpoint} 的清洗类；已注册：{sorted(registry)}")
    return registry[endpoint]

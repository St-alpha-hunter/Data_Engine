import time
from datetime import date

import psycopg
import pytest

"""

证券主表（ref.securities / ref.security_identifiers）测试：约束、resolve、按日期查询、当前视图
在测试库 data_engine_test 上运行（夹具见 conftest.py）

"""


def add_security(conn, name, security_type="common_stock", **kw):
    cols = ["name", "security_type", *kw]
    vals = [name, security_type, *kw.values()]
    sql = f"INSERT INTO ref.securities ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) RETURNING security_id"
    return conn.execute(sql, vals).fetchone()[0]


def add_id(conn, security_id, id_type, value, valid_from, valid_to=None, source="manual"):
    conn.execute(
        "INSERT INTO ref.security_identifiers (security_id, id_type, id_value, valid_from, valid_to, source) "
        "VALUES (%s, %s, %s, %s, %s, %s)", (security_id, id_type, value, valid_from, valid_to, source))


def resolve(conn, id_type, value, d):
    return conn.execute("SELECT ref.resolve_security(%s, %s, %s)", (id_type, value, d)).fetchone()[0]


@pytest.fixture
def sample(conn):
    """美联银行 / 微博 共用 WB；Meta 从 FB 改名 META"""
    wachovia = add_security(conn, "Wachovia Corp", status="delisted", last_trade_date=date(2008, 12, 31))
    weibo = add_security(conn, "Weibo Corporation", "adr", country="CN")
    meta = add_security(conn, "Meta Platforms, Inc.")
    add_id(conn, wachovia, "symbol", "WB", date(1996, 1, 2), date(2009, 1, 2))
    add_id(conn, weibo, "symbol", "WB", date(2014, 4, 17))
    add_id(conn, meta, "symbol", "FB", date(2012, 5, 18), date(2022, 6, 9))
    add_id(conn, meta, "symbol", "META", date(2022, 6, 9))
    add_id(conn, meta, "cik", "0001326801", date(2012, 5, 18))
    return {"wachovia": wachovia, "weibo": weibo, "meta": meta}


# =========================
# ✅ resolve：代码 + 日期 → 证券
# =========================
def test_resolve_reused_symbol(conn, sample):
    assert resolve(conn, "symbol", "WB", date(2008, 6, 30)) == sample["wachovia"]
    assert resolve(conn, "symbol", "WB", date(2026, 10, 1)) == sample["weibo"]
    assert resolve(conn, "symbol", "WB", date(2011, 1, 3)) is None          # 两段之间没人用


def test_resolve_symbol_change_boundary(conn, sample):
    """有效期 [from, to)：交界日 2022-06-09 属于 META，不属于 FB"""
    assert resolve(conn, "symbol", "FB", date(2022, 6, 8)) == sample["meta"]
    assert resolve(conn, "symbol", "FB", date(2022, 6, 9)) is None
    assert resolve(conn, "symbol", "META", date(2022, 6, 9)) == sample["meta"]


def test_securities_asof_and_current_view(conn, sample):
    asof = {r[0]: r[1:] for r in conn.execute("SELECT * FROM ref.securities_asof('2020-01-02')")}
    assert asof[sample["meta"]][:2] == ("FB", "0001326801")
    assert asof[sample["weibo"]][0] == "WB"
    assert sample["wachovia"] not in asof                                    # 2020 年已经没有有效代码

    cur = {r[0]: r for r in conn.execute("SELECT security_id, symbol, status FROM ref.v_securities_current")}
    assert cur[sample["meta"]][1:] == ("META", "active")
    assert cur[sample["wachovia"]][1:] == (None, "delisted")


# =========================
# ✅ 约束
# =========================
def test_same_symbol_same_time_rejected(conn, sample):
    with pytest.raises(psycopg.errors.ExclusionViolation, match="exc_security_identifiers_value_one_security"):
        add_id(conn, sample["meta"], "symbol", "WB", date(2020, 1, 1))


def test_two_symbols_same_security_same_time_rejected(conn, sample):
    with pytest.raises(psycopg.errors.ExclusionViolation, match="exc_security_identifiers_one_value_per_type"):
        add_id(conn, sample["meta"], "symbol", "FBX", date(2020, 1, 1), date(2021, 1, 1))


@pytest.mark.parametrize("id_type, value, constraint", [
    ("cik", "1326801", "chk_security_identifiers_cik_format"),          # 不足 10 位
    ("symbol", "meta", "chk_security_identifiers_symbol_format"),       # 小写
    ("cusip", "30303M10", "chk_security_identifiers_cusip_format"),     # 8 位
    ("isin", "US30303M1027X", "chk_security_identifiers_isin_format"),  # 13 位
])
def test_identifier_formats(conn, sample, id_type, value, constraint):
    with pytest.raises(psycopg.errors.CheckViolation, match=constraint):
        add_id(conn, sample["meta"], id_type, value, date(2030, 1, 1))


def test_identifier_formats_valid(conn, sample):
    add_id(conn, sample["meta"], "cusip", "30303M102", date(2012, 5, 18))
    add_id(conn, sample["meta"], "isin", "US30303M1027", date(2012, 5, 18))
    add_id(conn, sample["weibo"], "symbol", "BRK.B", date(2000, 1, 3), date(2001, 1, 2))   # 带点的类别股代码


@pytest.mark.parametrize("kw, constraint", [
    ({"status": "delisted"}, "chk_securities_delisted_iff_last_date"),
    ({"last_trade_date": date(2020, 1, 1)}, "chk_securities_delisted_iff_last_date"),
    ({"delisting_reason": "acquired"}, "chk_securities_delisting_reason"),
    ({"primary_exchange": "NYSE"}, "chk_securities_exchange_mic"),
    ({"country": "USA"}, "chk_securities_country"),
    ({"review_status": "ok"}, "chk_securities_review_status"),
    ({"first_trade_date": date(2020, 1, 2), "status": "delisted", "last_trade_date": date(2019, 1, 2)}, "chk_securities_trade_dates"),
])
def test_securities_constraints(conn, kw, constraint):
    with pytest.raises(psycopg.errors.CheckViolation, match=constraint):
        add_security(conn, "X", **kw)


def test_security_type_rejected(conn):
    with pytest.raises(psycopg.errors.CheckViolation, match="chk_securities_type"):
        add_security(conn, "X", "stock")


def test_identifier_requires_existing_security(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        add_id(conn, 999999, "symbol", "ZZZ", date(2020, 1, 1))


# =========================
# ✅ 默认值 / updated_at
# =========================
def test_defaults_and_updated_at(conn):
    sid = add_security(conn, "Apple Inc.")
    status, review, created, updated = conn.execute(
        "SELECT status, review_status, created_at, updated_at FROM ref.securities WHERE security_id = %s", (sid,)).fetchone()
    assert (status, review) == ("active", "needs_review")          # 新建默认待人工核对
    time.sleep(0.01)
    conn.execute("UPDATE ref.securities SET review_status = 'verified' WHERE security_id = %s", (sid,))  # 单独一个事务
    new_updated = conn.execute("SELECT updated_at FROM ref.securities WHERE security_id = %s", (sid,)).fetchone()[0]
    assert new_updated > updated

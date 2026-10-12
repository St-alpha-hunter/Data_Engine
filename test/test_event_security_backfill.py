from datetime import date

from pipelines.event_security_backfill import (
    Master, map_index_changes, map_party, map_per_symbol, map_symbol_changes,
)

"""

事件回填 security_id 的对应规则（纯函数，用内存里的小主表）

"""

D = date.fromisoformat


def master():
    m = Master()
    rows = [  # security_id, 代码, 开始, 结束
        (1, "GOOG", "2004-08-19", "2014-04-03"), (1, "GOOGL", "2014-04-03", None),     # A 类股
        (2, "GOOG", "2014-04-03", None),                                              # C 类股
        (3, "FB", "2012-05-18", "2022-06-09"), (3, "META", "2022-06-09", None),
        (4, "STI", "1996-01-02", "2019-12-09"),                                       # SunTrust，代码后来给了别家
        (5, "POM", "2007-11-09", "2016-03-24"), (6, "EXC", "2001-01-02", None),
    ]
    for sid, sym, a, b in rows:
        m.symbol[sym].append((sid, D(a), D(b) if b else None))
    m.cik["0001652044"] = [(1, D("2004-08-19"), None), (2, D("2014-04-03"), None)]   # 两类股共用 CIK
    m.cik["0000320193"] = [(3, D("2012-05-18"), None)]
    m.life = {1: (D("2004-08-19"), None), 2: (D("2014-04-03"), None), 3: (D("2012-05-18"), None),
              4: (D("1996-01-02"), D("2019-12-06")), 5: (D("2007-11-09"), D("2016-03-23")), 6: (D("2001-01-02"), None)}
    m.last_symbol = {1: "GOOGL", 2: "GOOG", 3: "META", 4: "STI", 5: "POM", 6: "EXC"}
    return m


def ev(eid, symbol, d, **kw):
    return {"event_id": eid, "symbol": symbol, "d": D(d), **kw}


def test_per_symbol_rules():
    out = map_per_symbol([
        ev(1, "GOOG", "2010-01-04"),       # 当天 GOOG 是 A 类股
        ev(2, "GOOG", "2020-01-02"),       # 当天 GOOG 是 C 类股
        ev(3, "META", "2019-01-02"),       # FMP 按现在的代码记历史：改名前也记在 META 下
        ev(4, "META", "2005-01-03"),       # 早于存续期：不填
        ev(5, "STI", "2015-01-02"),        # 代码后来给了别家，但别家 2022 年才上市：填
        ev(6, "STI", "2023-01-03"),        # 别家上市以后：不填
    ], master(), reused={"STI": D("2022-05-02")})
    assert {k: v[0] for k, v in out.items()} == {1: 1, 2: 2, 3: 3, 4: None, 5: 4, 6: None}


def test_index_changes_disagreement_left_empty():
    rows = [ev(10, "GOOGL", "2006-04-03", action="add"),     # 代码被回溯：当天没人用 GOOGL → 用成分区间对应
            ev(11, "FB", "2013-12-23", action="add"),
            ev(12, "FB", "2022-06-09", action="remove"),     # 调出用前一天
            ev(13, "GOOG", "2006-04-03", action="add")]      # 当天用代码的是 1，成分区间说是 2 → 不填
    out = map_index_changes(rows, master(), by_candidate={10: 1, 11: 3, 12: 3, 13: 2})
    assert {k: v[0] for k, v in out.items()} == {10: 1, 11: 3, 12: 3, 13: None}


def test_symbol_changes_and_ignore():
    rows = [{"event_id": 20, "old_symbol": "FB", "new_symbol": "META", "d": D("2022-06-09")},
            {"event_id": 21, "old_symbol": "POM", "new_symbol": "EXC", "d": D("2016-03-22")}]
    out = map_symbol_changes(rows, master(), ignore={("POM", "EXC", D("2016-03-22"))})
    assert out[20][0] == 3 and out[21] == (None, "人工标记忽略")


def test_party_cik_only_when_unique():
    m = master()
    assert map_party(m, "XYZ", "0000320193", D("2020-01-02"))[0] == 3
    assert map_party(m, "XYZ", "0001652044", D("2020-01-02"))[0] is None          # 两只共用，拿不准
    assert map_party(m, "FB", None, D("2020-01-02")) == (3, "文件日用这个代码")

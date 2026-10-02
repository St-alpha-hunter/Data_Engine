import pandas as pd

def cumpord(fields:list[str]):
    def wrap(x:pd.DataFrame):
        x.loc[:,fields] = x[fields].cumsum()
        return x
    return wrap


def excutte(sel)
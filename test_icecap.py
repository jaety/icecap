import icecap as ice
from icecap import Project
import polars as pl

def test_create():
    proj = Project()
    df = proj.DataFrame({'a':[0,1,2]})
    assert type(df) == ice.Frame

def test_naming():
    proj = Project()
    proj['a'] = proj.DataFrame({'a':[0,1,2]})
    assert list(*(proj.named.keys()))[0] == 'a'

def test_copy():
    df = Project().DataFrame({'a':[0,1,2]})
    df2= df.copy()
    assert type(df2)==ice.Frame

def test_df():
    proj = Project()
    df_a = proj.DataFrame({'a':[1,2,3]})
    df_b = df_a.with_columns(b = pl.col('a')+1)

    assert df_b.parent == df_a

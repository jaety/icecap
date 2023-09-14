import icecap as ice
from icecap import pl

def test_copy():
    df = ice.DataFrame()
    df2= df.copy()
    assert type(df2)==ice.DataFrame

def test_df():
    df = pl.DataFrame({'a':[1,2,3]})
    df = df.with_columns(b = pl.col('a')+1)

    """
    df(.., "with_columns", [], {'b':...})
        df(.., "DataFrame", [{'a':...}], {})
    """
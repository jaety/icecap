# Backlog

* Table Formatters
* Server & instant update visuals
* More seamless to go between lazy and eager
* connect the pipes
* time series operations

# Server Architecture

I'll launch a server that is watching a directory for changes.
When a change happens, it'll diff against existing objects and rerun what's changed.

```
X = Table()
X *= "a", range(3)
X *= "b", range(2)
X += "c", X['a'] + X['b']

Y = Table()
Y *= X["a"].unique()
```

# Caching

Serialize will create an inspectable version of the logical plan, one that could be diffed,
but it's not quite right. I need to be able to excise pieces of it. and I need to be able
to point it to pre-cached references of pieces of it.

I was just looking at the wrong tool. Use 'explain()' and then string matching. 

# Formatting

Use Pandas for Table formatting. It's much richer
See https://pandas.pydata.org/docs/user_guide/style.html#Formatting-the-Display

Consider (later) auto truncating to avoid too expensive transitions.


# Syntactic Sugar

What do we need beyond polars itself?

## Misc Notes

```
table Input2
    def Country    from Input.Country
    def Date_Field from Fields(Input).where("^[0-9]+.*$")
    def Date = Date_Field.map_alias(lambda s: s[:4])
    def GDP  = Input.FindRow(_.Country = Country).GetField(Date_Field).cast(pl.Float32, strict=False)
```

```
input2 = Table()
input["Country"]    = Explode(Input.select("Country"))
input["Date_Field"] = Explode(Input.fields().where("^[0-9]+.*$"))
input["Date"]       = input["Date_Field"].map_alias(lambda s: s[:4])
input["GDP"]        = Input.filter(Input["Country"] == input2["Country"]) ??? select(Input2["Date_Field"])
```

## Graph

Node: (Source: Node, Args: [Any], Kwargs: {String:Any})

Node1 -> Node2 -> Node3


Node
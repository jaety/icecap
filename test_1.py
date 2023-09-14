from polars_utils import Pipe, inspect_df, pl

input = pl.scan_csv("world_bank_gdp_data.csv").tracer()

input = input.select(
    pl.col("Country Code").alias("Country"),
    pl.col("^[0-9]+.*$").cast(pl.Float32, strict=False).map_alias(lambda s: s[:4]))

input = input.melt(id_vars=["Country"], variable_name="Date", value_name="GDP") #, "pivot tall"

input = input.select(
    pl.col("Country"),
    pl.col("Date").cast(pl.Int32),
    pl.col("*").exclude("Country", "Date")
) #, "cast dates to number"

input = input.drop_nulls().sort(by=["Country", "Date"]) #, "drop nulls and sort"

input.name = "input"

# inspect_df(input, lambda x: x.head().collect())


# input_limited = input.filter(pl.col("Country")=="USA")


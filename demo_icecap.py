# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
from icecap import Project
import icecap as ice
import polars as pl 

# %%
proj = Project()

# %%
input = proj.scan_csv("world_bank_gdp_data.csv")

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



# %%
widget = input.inspect(adjust_lf=lambda x: x.head(5).collect())
widget

# %%

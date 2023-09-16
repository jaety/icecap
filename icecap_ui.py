"""
A viewer for polars provenance information
"""

import streamlit as st
import pandas as pd
import numpy as np
import polars_utils as pu

import test_1 
st.set_page_config(page_title="Icecap Viewer", layout="wide")

st.title("Icecap UI")

def objects_in(src):
    attrs = dir(src)
    result= {}
    for attr in attrs:
        value = getattr(src, attr)
        if isinstance(value, pu.LazyFrameBase):
            result[attr] = value
    return result

names = objects_in(test_1)


left,center,right = st.columns([1,3,4])

with left:
    selected_df = st.radio("Choose Frame", names.keys(), horizontal=False)
    steps = pu.traverse(names[selected_df])

with center:

    selected_step = st.slider("step", 0, len(steps)-1, len(steps)-1)

    st.markdown(pu.pipe_html(names[selected_df], highlight=selected_step), unsafe_allow_html=True)

with right:

    st.write(steps[selected_step].head(10).collect().to_pandas())
import polars as pl
from functools import update_wrapper

"""
Question here is whether I care that it's a LazyFrame
or a DataFrame

I think the difference is that the DataFrame will
execute immediately, so I should capture the value
"""
def is_polars_frame(obj):
    return isinstance(obj, pl.LazyFrame) or isinstance(obj, pl.DataFrame)

class Frame:
    def copy(self):
        return self.__class__()        

class DataFrame(Frame):
    def __init__(self, parent, parent_attr, args, kwargs, value):
        self.parent = parent 
        self.parent_attr = parent_attr
        self.args = args 
        self.kwargs = kwargs 
        self.value = value 
        
    pass

class DataFrameMethod:
    def __init__(self, node, attr):
        self.node, self.attr = node, attr 
    
    def __call__(self, *args, **kwargs):
        pass 

class LazyFrame(Frame):
    def __init__(self, parent, parent_attr, args, kwargs, value):
        self.parent = parent 
        self.parent_attr = parent_attr
        self.args = args 
        self.kwargs = kwargs 

        self.value = value 

class LazyFrameMethod:
    def __init__(self, node, attr):
        self.node, self.attr = node, attr 
        
    def __call__(self, *args, **kwargs):
        pass 


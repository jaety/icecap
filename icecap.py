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

def prep_arg(arg):
    return arg() if isinstance(arg,Frame) else arg 

def prep_args(args, kwargs):
    args = [prep_arg(a) for a in args]
    kwargs = {k:prep_arg(v) for k,v in kwargs.items()}
    return args,kwargs

def traverse(df, reverse=False):
    result = []
    while hasattr(df, 'parent'):
        result.append(df)
        df = df.parent
    return result[::-1] if not reverse else result 

class Frame:
    def __init__(self, wrapped, parent, parent_attr, args, kwargs):
        self.wrapped = wrapped

        self.parent = parent 
        self.parent_attr = parent_attr
        self.args = args 
        self.kwargs = kwargs 

    def __getattr__(self, attr):
        if hasattr(self.wrapped, attr):
            property = getattr(self.wrapped, attr)
            if callable(property):
                return FrameMethod(self, attr)
            else:
                return property
        else:
            raise AttributeError(f"Frame has no attribute {attr}")

    def _repr_html_(self):
        return self.wrapped._repr_html_()
    
    def copy(self):
        return self.__class__()        

    def inspect(self, adjust_df=lambda x:x, adjust_lf=lambda x:x):
        inspect_df(self, adjust_df, adjust_lf)

class FrameMethod:
    def __init__(self, frame, attr):
        self.frame, self.attr = frame, attr 
        self.method = getattr(self.frame.wrapped, self.attr)
        update_wrapper(self, self.method)
    
    def __call__(self, *args, **kwargs):
        p_args, p_kwargs = prep_args(args, kwargs)
        value = self.method(*p_args, **p_kwargs)
        return Frame(value, self.frame, self.attr, args, kwargs) 

class Project:
    def __init__(self):
        self.wrapped = pl 

    def __dir__(self):
        return self.wrapped.__dir__()

    def __getattr__(self, attr):
        if hasattr(self.wrapped, attr):
            property = getattr(self.wrapped, attr)
            if callable(property):
                return FrameMethod(self, attr)
            else:
                return property
        else:
            raise AttributeError(f"Frame has no attribute {attr}")

#################################
# Visuals        
#################################

def arg_format(a):
    if isinstance(a, str):
        return f'"{str(a)}"'
    else:
        return str(a)

def signature(df, prefix=""):
    arg_strs = [arg_format(a) for a in df.args]
    arg_str  = ", ".join(arg_strs)
    kw_strs  = [f"{k}={str(v)}" for k,v in df.kwargs.items()]
    kw_strs  = ", ".join(kw_strs)
    all_args = ", ".join([s for s in [arg_str, kw_strs] if len(s) > 0])
    return f"{prefix}{df.parent_attr}({all_args})" 

def pipe_str(df, reverse=False):
    df_list = traverse(df, reverse)
    if reverse:
        sigs = [signature(d, "  "*i) for i,d in enumerate(df_list)]
    else:
        sigs = [signature(d, "" if i==0 else "  .") for i,d in enumerate(df_list)]
    return "\n".join(sigs)

def pipe_html(df, reverse=False, highlight=None):
    df_list = traverse(df, reverse)
    shared_styles = """padding:0px; margin:5px; font-family: "Lucida Console", Monospace; font-size:1em"""
    sigs = []
    for i,d in enumerate(df_list):
        text_indent = i*20 if reverse else (0 if i==0 else 20)
        prefix = "" if reverse else ("" if i==0 else ".")
        font_weight = "bold" if highlight==i else "normal"
        sigs.append(f"<p style='text-indent:{text_indent}px; font-weight:{font_weight}; {shared_styles}'>{signature(d, prefix)}</p>")
    return "\n".join(sigs)

def inspect_df(df, adjust_df=lambda x:x, adjust_lf=lambda x:x):
    # These are internalized because only applicatble in jupyter. Better pattern for this?
    from ipywidgets import interactive, interact
    import ipywidgets as widgets
    from IPython.display import display, HTML

    stack = traverse(df)
    min_ = 0 
    max_ = len(stack)-1
    slider = widgets.IntSlider(min=min_, max=max_, step=1, value=max_)
    trace_out = widgets.Output()
    df_out    = widgets.Output() # TODO: Have overflow work the way I want
    widget = widgets.HBox([
                widgets.VBox([slider, trace_out], layout=widgets.Layout(min_width='320px')),
                df_out
             ])

    def update_trace(event):
        i = event if isinstance(event,int) else event['new']
        trace_out.clear_output(wait=True)
        with trace_out:
            display(HTML(pipe_html(df, highlight=i)))
    slider.observe(update_trace, 'value')

    def update_output(event):
        i = event if isinstance(event,int) else event['new']
        df_out.clear_output(wait=True)
        with df_out:
            obj = stack[i].wrapped
            adjust = adjust_lf if isinstance(obj, pl.LazyFrame) else adjust_df
            display(adjust(obj))
    slider.observe(update_output, 'value')

    update_trace(max_)
    update_output(max_)
    display(widget)


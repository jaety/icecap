import polars as pl
from functools import update_wrapper

pl.DataFrame.collect = lambda self: self

def is_polars_frame(obj):
    return isinstance(obj, pl.LazyFrame) or isinstance(obj, pl.DataFrame)

def prep_arg(arg):
    if isinstance(arg, Frame):
        return arg.wrapped 
    else:
        return arg
    
def prep_args(args, kwargs):
    args = [prep_arg(a) for a in args]
    kwargs = {k:prep_arg(v) for k,v in kwargs.items()}
    return args,kwargs
class Frame:
    def __init__(self, wrapped, parent, parent_attr, args, kwargs, project=None):
        self.wrapped = wrapped

        self.parent = parent 
        self.parent_attr = parent_attr
        self.args = args 
        self.kwargs = kwargs 

        self.project = project
        self.name = None

        self._over_context = None

        if self.project is not None:
            self.project.add(self)
    
    def over(self, over_expr):
        return Over(self, over_expr)

    def __getattr__(self, attr):
        if hasattr(self.wrapped, attr):
            property = getattr(self.wrapped, attr)
            if callable(property):
                return FrameMethod(self, attr)
            else:
                return property
        else:
            raise AttributeError(f"Frame has no attribute {attr}")
        
    def __setitem__(self, key, expr):
        if self._over_context:
            expr = expr.over(self._over_context)
        self.inplace().with_columns(**{key:expr})

    def _repr_html_(self):
        return self.wrapped._repr_html_()
    
    def inplace(self):
        return InPlace(self)

    def copy(self):
        return Frame(self.wrapped, self, "copy", [], {}, self.project)
    
    def true_copy(self):
        return Frame(self.wrapped, self.parent, self.parent_attr, self.args, self.kwargs)

    def inplace_update(self, new_parent, new_frame):
        self.wrapped = new_frame.wrapped
        self.parent  = new_parent 
        self.parent_attr = new_frame.parent_attr 
        self.args = new_frame.args 
        self.kwargs = new_frame.kwargs 
    
    def inspect(self, adjust=None, reversed=False, named_only=False, until=None):
        return inspect_df(self, adjust, reversed=reversed, named_only=named_only, until=until)

class FrameMethod:
    def __init__(self, frame, attr, inplace=False):
        self.frame, self.attr = frame, attr 
        self.method = getattr(self.frame.wrapped, self.attr)
        self.inplace = inplace
        update_wrapper(self, self.method)
    
    def __call__(self, *args, **kwargs):
        p_args, p_kwargs = prep_args(args, kwargs)
        value = self.method(*p_args, **p_kwargs)
        top_frame = Frame(value, self.frame, self.attr, args, kwargs, project=self.frame.project)
        if self.inplace:
            new_parent = self.frame.true_copy()
            self.frame.inplace_update(new_parent, top_frame)
            return self.frame
        else:
            return top_frame

class InPlace:
    def __init__(self, frame):
        self.frame = frame 

    def __getattr__(self, attr):
        if hasattr(self.frame.wrapped, attr):
            return FrameMethod(self.frame, attr, inplace=True)
        else:
            AttributeError(f"Frame has no attribute {attr}")

class Over:
    def __init__(self, frame, over_expr):
        self.frame = frame 
        self.over_expr = over_expr 

    def __enter__(self):
        self.frame._over_context = self.over_expr
    def __exit__(self, type, value, traceback):
        self.frame._over_context = None

class Project:
    def __init__(self):
        self.wrapped = pl 
        self.frames  = []
        self.named   = {}
        self.name_lookup = {}
        self.project = self

    def __dir__(self):
        return self.wrapped.__dir__()
    
    def add(self, frame):
        self.frames.append(frame)

    def name_for(self, frame):
        return self.name_lookup.get(frame, None)

    def __setitem__(self, key, value):
        self.named[key] = value 
        value.name = key
        self.name_lookup[value] = key
    
    def __getitem__(self,key):
        return self.named[key]

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

def traverse(df, reverse=False, named_only=False, until=None):
    result = []
    if isinstance(until, str):
        stop_at = lambda x: x.name == until 
    else:
        stop_at = until if until else lambda x: False
    while hasattr(df, 'parent'):
        if not named_only or df.name:
            result.append(df)
        if stop_at(df):
            break
        df = df.parent
    return result[::-1] if not reverse else result 

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

def pipe_html(df, reverse=False, highlight=None, named_only=False):
    df_list = traverse(df, reverse) if isinstance(df, Frame) else df
    shared_styles = """padding:0px; margin:5px; font-family: "Lucida Console", Monospace; font-size:1em"""
    sigs = []
    for i,d in enumerate(df_list):
        if (named_only and d.name) or not named_only:
            name = f"[{d.name}] " if d.name else ""
            text_indent = i*20 if reverse else (0 if i==0 else 20)
            prefix = "" if reverse else ("" if i==0 else ".")
            font_weight = "bold" if highlight==i else "normal"
            sigs.append(f"<p style='text-indent:{text_indent}px; font-weight:{font_weight}; {shared_styles}'>{name}{signature(d, prefix)}</p>")
    return "\n".join(sigs)    

from functools import partial
def adjust_inner(df, filters={}, head=None, finalize=lambda df: df.collect()):
    out = df 
    for key,filter_expr in filters.items():
        if isinstance(key, tuple):
            colname, dtype = key 
        else:
            colname, dtype = key, None
        if colname in out.schema and (dtype is None or out.schema[colname] == dtype):
            out = out.filter(filter_expr)

    if head is not None:
        out = out.head(head)

    return finalize(out)

def adjust(filters={}, head=None, finalize=lambda df: df.collect()):
    return partial(adjust_inner, filters=filters, head=head, finalize=finalize)

def inspect_df(df, adjuster=None, reversed=False, named_only=False, until=None):
    # These are internalized because only applicatble in jupyter. Better pattern for this?
    from ipywidgets import interactive, interact
    import ipywidgets as widgets
    from IPython.display import display, HTML
    adjuster = adjuster if adjuster else adjust(head=10)

    stack = traverse(df, reverse=reversed, named_only=named_only, until=until)
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
            display(HTML(pipe_html(stack, highlight=i, reverse=reversed, named_only=named_only)))
    slider.observe(update_trace, 'value')

    def update_output(event):
        i = event if isinstance(event,int) else event['new']
        df_out.clear_output(wait=True)
        with df_out:
            obj = stack[i].wrapped
            display(adjuster(obj))
    slider.observe(update_output, 'value')

    update_trace(max_)
    update_output(max_)
    return widget


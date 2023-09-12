import polars as pl

"""
TODOs
* getattr working with intellisense
"""

def prep_arg(arg):
    return arg() if isinstance(arg,LazyFrameNode) else arg 

def prep_args(args, kwargs):
    args = [prep_arg(a) for a in args]
    kwargs = {k:prep_arg(v) for k,v in kwargs.items()}
    return args,kwargs

class LazyFrameNode:
    def __init__(self, parent, parent_attr, args=None, kwargs=None):
        self.parent      = parent
        self.parent_attr = parent_attr
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}

    def copy(self):
        return LazyFrameNode(self.parent, self.parent_attr, self.args, self.kwargs)

    def value(self):
        parent_obj = self.parent.value() if isinstance(self.parent, LazyFrameNode) else self.parent
        args, kwargs = prep_args(self.args, self.kwargs)
        return getattr(parent_obj, self.parent_attr)(*args, **kwargs)

    @property
    def inplace(self):
        return InPlace(self)

    def __getattr__(self, attr):
        value = self.value() 
        if hasattr(value, attr):
            if callable(getattr(value, attr)):
                return LazyFrameMethod(self, attr)
            else:
                return value
        else:
            raise AttributeError(f"Node has no attribute {attr}")
    
    def __setitem__(self, key, expr):
        self.parent = LazyFrameNode(self.parent, self.parent_attr, self.args, self.kwargs)
        self.parent_attr = 'with_columns'
        self.args = []
        self.kwargs = {key:expr}
        return self

    def _info(self, separator=" | "):
        return separator.join([repr(x) for x in [self.func, self.args, self.kwargs]])

    def group(self, expr):
        return Group(self, expr)

class Group:
    def __init__(self, node, grouping_expr):
        self.node = node
        self.grouping_expr = grouping_expr
    
    def __enter__(self):
        return self 

    def __exit__(self, type, value, traceback):
        pass 

    def __setitem__(self, key, expr):
        self.node[key] = expr.over(self.grouping_expr)
        return self 
            

class InPlace:
    def __init__(self, node):
        self.node = node
    
    def __getattr__(self, attr):        
        if hasattr(self.node.value(), attr):
            return LazyFrameMethod(self.node, attr, inplace=True)
        else:
            raise AttributeError(f"Node has no attribute {attr}")

class LazyFrameMethod:
    def __init__(self, node, attr, inplace=False):
        self.node = node
        self.attr = attr
        self.inplace = inplace

    def __call__(self, *args, **kwargs):
        if self.inplace:
            node = self.node
            node.args = args 
            node.kwargs = kwargs
        else:
            node = LazyFrameNode(self.node, self.attr, args, kwargs)
        value = node.value()
        if isinstance(value, pl.LazyFrame):
            return node 
        else:
            return value

def Pipe(obj):
    return PN(obj)

class PN:
    def __init__(self, obj):
        self.obj = obj

    def __dir__(self):
        return dir(self.obj)
    
    def __getattr__(self, attr):
        if hasattr(self.obj, attr):
            inner_attr = getattr(pl, attr)
            if callable(inner_attr):
                return LazyFrameMethod(pl, attr)
            else:
                return inner_attr

        else:
            raise AttributeError(f"pl has no method {attr}")

def traverse(df, reverse=False):
    result = []
    while hasattr(df, 'parent'):
        result.append(df)
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

def inspect_df(df, adjust=lambda x:x):
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
            display(adjust(stack[i]))
    slider.observe(update_output, 'value')

    update_trace(max_)
    update_output(max_)
    display(widget)

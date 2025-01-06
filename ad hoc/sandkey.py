import plotly.graph_objects as go
def sandkey(df:pd.DataFrame, flow_cat:list, weight_col:str,title:str='Flow Diagram' ,pd_calc:str='sum'):
    """Create sandkey plot.

    Args:
        df (pd.DataFrame): Data
        flow_cat (list): Columns to use as flow.
        weight_col (str): Column used for flow
        title (str, optional): Chart Title. Defaults to 'Flow Diagram'.
        pd_calc (str, optional): Summary calculation. Defaults to 'sum'.
    
    Example:
    category = ['start', 'state_type','lat_lon', 'land_type','action']

    sandkey(df, flow_cat=category,weight_col='num')

    """
    data_lst = []
    for i in range(len(flow_cat)-1):

        temp_df = df[[flow_cat[i],flow_cat[i+1],weight_col]]
        temp_df.columns = ['source','target', weight_col]
        data_lst.append(temp_df)
    data = pd.concat(data_lst)    
    data = data.groupby(['source','target']).agg(weight=('num',pd_calc)).reset_index()
    label_list = list(np.unique(df[flow_cat].values))

    fig = go.Figure(data=[go.Sankey(
        node = dict(
        pad = 15,
        thickness = 20,
        line = dict(color = "black", width = 0.5),
        label =label_list,
        color = "blue"
        ),
        link = dict(
            source = data['source'].apply(lambda x: label_list.index(x)),
            target = data['target'].apply(lambda x: label_list.index(x)),
            value = data['weight'].values.tolist()
    ))])
    fig.update_layout(title_text=title, font_size=10)
    fig.show()
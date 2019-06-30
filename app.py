import base64
import datetime
import io
import os 
from os import listdir
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import ntpath
import csv
import pandas as pd
# from extra import find_pfds_csv
from pfd import find_pfds_csv

from components import Header, make_dash_table, get_menu

DATA_FOLDER = "./data"


# important link https://dash.plot.ly/datatable/interactivity
# external CSS stylesheets
external_stylesheets = [
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    }
]



app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.config['suppress_callback_exceptions']=True
app.title = 'ANMAT'

DATA_FOLDER = "./data"
gdf = None
gresults = None
param_dict = dict()
param_dict["results_main_dir"] = "../Results/"




def read_table(tab_name):
    t_name = ntpath.basename(tab_name)
    try:
        df = pd.read_csv(filepath_or_buffer=tab_name, dtype=object, delimiter=',', low_memory=False,
                         quoting=csv.QUOTE_ALL, doublequote=True)
    except ValueError:
        try:
            df = pd.read_csv(filepath_or_buffer=tab_name, dtype=object, delimiter=',', low_memory=False,
                             quoting=csv.QUOTE_ALL, doublequote=True, encoding="ISO-8859-1")
        except:
            print("Error reading csv file .. file encoding is not recognizable")
            return None
    return df

def get_csv_files(folder):
    csv_files = []
    folder_contents = listdir(folder)
    for item in folder_contents:
        if item.endswith('.csv'):
            csv_files.append(item)
    return csv_files


def dynamic_page():
    return html.Div([
        Header(),
        html.Div([    
            html.Div([
                dcc.Dropdown(
                        id='uploaded-datasets',
                        options=[{'label': i, 'value': i} for i in get_csv_files(DATA_FOLDER)],
                        placeholder='Select a Dataset',
                    )
                ],style={
                        'width': '220px',
                        'display': 'inline-block',
                        'margin-left': '25px',
                    } 
                
            ), 
            html.Div([
                dcc.Upload(
                    id='upload-data',
                    children = html.Div([
                        html.Button(' Browse ', className='fa fa-upload')
                    ],style={
                        'backgroundColor':'green',
                        'color':'white',
                        'margin-left': '5px',
                    }),
                    
                    multiple=False
                ),
            ]),
            html.Div([
                dcc.Input(
                    placeholder='Enter the Confidence',
                    type='text',
                    value='',
                    id='Confidence'
                )
                ],style={
                        'width': '200',
                        'display': 'inline-block',
                        'margin-left': '5px',
                    }),
            html.Div([
                dcc.Input(
                    placeholder='Enter the Allowed Violations',
                    type='text',
                    value='',
                    id='Delta'
                )
                ],style={
                        'width': '200',
                        'display': 'inline-block',
                        'margin-left': '5px',
                    }),
            html.Div([
                dcc.Input(
                    placeholder='Enter the Min. Coverage',
                    type='text',
                    value='',
                    id='Coverage'
                )
                ],style={
                        'width': '200px',
                        'display': 'inline-block',
                        'margin-left': '5px',
                    }),
            html.Button('PFD Discovery', className='fa', id='button', 
                style={
                        'backgroundColor':'green',
                        'color':'white',
                        'width': '200',
                        'flow':'right',
                        'margin-left': '15px',
                    }),
        ], className="row",
            style={
                'width': '100%',
                'height':'50px',
                'borderWidth': '1px',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin-left': '25px',
                'margin-top': '10px',
            }),
        html.Div(id='output-data-upload'),
        html.Div(id='output-data-dropdown',
            style={
                'width': '100%',
                'height': '440px',
                'borderWidth': '1px',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin-left': '50px',
                'margin-right': '25px',
                'overflowY': 'scroll',
            }),
        html.Hr(),  # horizontal line
        html.Div(id = 'output-results',
            style={
                'width': '100%',
                'height': '200px',
                'borderWidth': '1px',
                'borderRadius': '5px',
                'textAlign': 'left',
                'margin-left': '25px',
                'margin-right': '25px',
                'margin-top': '40px'
            }),
        # Footer()
    ], className='body')

app.layout = dynamic_page



def upload_contents(contents, filename):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    new_file_name = ''
    try:
        if filename.endswith('csv'):
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
            if filename.endswith('xls'):
                new_file_name = filename.replace('.xls', '.csv')
            if filename.endswith('xlsx'):
                new_file_name = filename.replace('.xlsx', '.csv')
        filename2 = os.path.join(DATA_FOLDER, filename)
        df.to_csv(filename2, sep=',', encoding='latin', index=False, quoting=csv.QUOTE_ALL, doublequote=True)
    except Exception as e:
        print(e)
        return html.Div([
            ''
        ])
    return html.Div([
            ''
        ])


def parse_contents(filename):
    global gdf
    try:
        if filename.endswith('csv'):
            # Assume that the user uploaded a CSV file
            filename2 = os.path.join(DATA_FOLDER, filename)
            df = read_table(filename2)
            gdf = df
        # elif 'xls' in filename:
        #     # Assume that the user uploaded an excel file
        #     df = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])

    return html.Div([
        dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0,
                'textAlign':'left'
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'overflowY': 'scroll'
            },
            pagination_settings={
            "current_page": 0,
            "page_size": 50,
            },
        ),
        html.Hr(),  # horizontal line
    ], className='ui-grid-resize-columns ui-grid-pagination')


@app.callback([Output('output-data-upload', 'children'),
                Output('uploaded-datasets', 'options'),
                Output('uploaded-datasets', 'value')],
              [Input('upload-data', 'contents')],
              [State('upload-data', 'filename'),
               State('upload-data', 'last_modified')])
def update_output_data(content, fname, modified):
    if content:
        grid = upload_contents(content, fname)
        options=[{'label': i, 'value': i} for i in get_csv_files(DATA_FOLDER)]
        value=fname
        return grid, options, value
    else:
        options=[{'label': i, 'value': i} for i in get_csv_files(DATA_FOLDER)]
        return html.Div(['']), options, ''


@app.callback(Output('output-data-dropdown', 'children'),
              [Input('uploaded-datasets', 'value')])
def output_dropdown(fname):
    options=[{'label': i, 'value': i} for i in get_csv_files(DATA_FOLDER)]
    if fname:
        grid = parse_contents(fname)
        return grid
    else:
        return html.Div([''])
        
@app.callback(
    Output('output-results', 'children'),
    [Input('button', 'n_clicks')],
    [State('uploaded-datasets', 'value'),
    State('Confidence', 'value'),
    State('Delta', 'value'),
    State('Coverage', 'value')])
def update_output_discovery(n_clicks, fname, conf, delta, min_coverage):
    
    global gresults

    if fname:
        if conf:
            if delta:
                if min_coverage:
                    param_dict["tab_name"] = os.path.join(DATA_FOLDER, fname)
                    param_dict["min_acceptable_coverage"] = float(min_coverage) / 100.0 
                    param_dict["confidence_K"] = float(conf)
                    param_dict["allowed_noise_delta"] = float(delta)
                    gresults = find_pfds_csv(param_dict)
                    
                    return html.Div([
                        dcc.Tabs(
                            id="tabs-with-classes",
                            value='patt',
                            parent_className='custom-tabs',
                            className='tab',
                            children=[
                                dcc.Tab(
                                    label='Patterns',
                                    value='patt',
                                    className='custom-tab',
                                    selected_className='custom-tab--selected'
                                ),
                                dcc.Tab(
                                    label='PFDs',
                                    value='pfds',
                                    className='custom-tab',
                                    selected_className='custom-tab--selected'
                                ),
                                dcc.Tab(
                                    label='Violations',
                                    value='vio', className='custom-tab',
                                    selected_className='custom-tab--selected'
                                ),
                            ]),
                        html.Div(id='tabs-content-classes')
                    ])
                else:
                    return html.Div(['The min_coverage is missing'])
            else:
                return html.Div(['The allowed violations is missing'])
        else:
            html.Div(['The confidence is missing'])
    else:
        html.Div(['The data file is missing'])
    if n_clicks:
        return html.Div(['Something goes wrong after {}'.format(n_clicks)])
    return html.Div([''])





@app.callback(Output('tabs-content-classes', 'children'),
              [Input('tabs-with-classes', 'value')],
              [State('uploaded-datasets', 'value')])
def render_content(tab, tab_name):
    global gdf, gresults
    
    if tab == 'patt':
        att_names = gdf.columns.tolist()
        if gresults:
            df_details = gresults['df_details']
        tok_or_ngrams = dict()
        tok_or_ngrams.clear()
        for d in df_details.keys():
            tok_or_ngrams[df_details[d]['att_name']] = df_details[d]['tg_vs_ng']
        data = []
        cols = ['Attributes', 'Tokens or n-Grams']
        for i in range(len(data)):
            data.remove(data[0])
        gms = gresults['patterns']
        for k in tok_or_ngrams.keys():
            new_k = gdf.columns.get_loc(k)
            # print(new_k, gms.keys())
            if new_k in gms.keys():
                data.append([k, tok_or_ngrams[k]])
            else:
                data.append([k, '----'])
        att_df = pd.DataFrame(data, columns=cols)
        return html.Div([
            dash_table.DataTable(
            data=att_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in att_df.columns],
            id='patterns-table',
            row_selectable="single",
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': '500px',
                'textAlign':'left',
                'font-size': '150%',
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'maxWidth':'600px',
                'overflowY': 'scroll',
                'margin-left': '20px',
                # 'border': 'thin lightgrey solid',
            }
        ),
            # html.H3('Select an attribute to see the patterns extracted from the attribute.'),
            html.Div(id='patterns-container', className="six columns"), 
        ], className="row ")

    elif tab == 'pfds':
        # att_names = gdf.columns.tolist()
        if gresults:
            pfds = gresults['pfds']
        data = []
        cols = ['Determinant', 'Dependent']
        for i in range(len(data)):
            data.remove(data[0])
        for pfd in pfds:
            data.append([pfd['det'], pfd['dep']])
        pfds_df = pd.DataFrame(data, columns=cols)
        return html.Div([
            dash_table.DataTable(
            data=pfds_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in pfds_df.columns],
            id='pfds-table',
            row_selectable="single",
            row_deletable=True,
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': '500px',
                'textAlign':'left',
                'font-size': '150%',
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'maxWidth':'600px',
                'overflowY': 'scroll',
                'margin-left': '20px',
                # 'border': 'thin lightgrey solid',
            }
        ),
            # html.H3('Select a dependency to see its tableau of PFDs'),
            html.Div(id='pfds-container', className="six columns"),
            html.Div(id='pfds-container-hidden', style={'display':'none'}),
        ], className="row ")

    elif tab == 'vio':
        if gresults:
            pfds = gresults['pfds']
        data = []
        cols = ['Determinant', 'Dependent']
        for i in range(len(data)):
            data.remove(data[0])
        for pfd in pfds:
            data.append([pfd['det'], pfd['dep']])
        pfds_df = pd.DataFrame(data, columns=cols)
        return html.Div([
            dash_table.DataTable(
            data=pfds_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in pfds_df.columns],
            id='vios-table',
            row_selectable="single",
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': '500px',
                'textAlign':'left',
                'font-family': 'Times New Roman',
                'font-size': '150%',
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'maxWidth':'600px',
                'overflowY': 'scroll',
                'margin-left': '20px',
                # 'border': 'thin lightgrey solid',
            }
        ),
            html.Div(id='vios-container', className="six columns"), 
        ], className="row ")


@app.callback(
    Output('patterns-container', "children"),
    [Input('patterns-table', "derived_virtual_data"),
     Input('patterns-table', "derived_virtual_selected_rows")])
def update_graphs_patterns(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    global gresults, gdf
    # print(derived_virtual_selected_rows)
    if not(derived_virtual_selected_rows):
        derived_virtual_selected_rows = []
        return html.Div([
            html.H4('') 
        ], className="six columns")
    else:
        gms = gresults['patterns']
        if derived_virtual_selected_rows[0] in gms.keys():
            req_gms = gms[derived_virtual_selected_rows[0]]
            patt_df = pd.DataFrame(req_gms, columns=['patterns', 'frequency'])
            return html.Div([
                dash_table.DataTable(
                data=patt_df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in patt_df.columns],
                id='patterns-freq-table',
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
                }],
                style_cell={
                    'whiteSpace': 'no-wrap',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxWidth': '600px',
                    'textAlign':'left',
                    'font-size': '150%',
                    'font-family': 'Times New Roman'
                },
                style_cell_conditional=[{
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)'
                }],
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold'
                },
                style_table={
                    'max_rows_in_viewport':15,
                    'maxHeight': '400px',
                    'maxWidth':'800px',
                    'overflowY': 'scroll',
                    'margin-left': '20px',
                    # 'border': 'thin lightgrey solid',
                }
            ),
                # html.H3(gdf.columns[derived_virtual_selected_rows] + '   Just for test')
            ])
        else:
            text = '(' + gdf.columns[derived_virtual_selected_rows[0]] + ') has been ignored because it represents ' 
            text += 'a numerical quantity '
            return html.Div([
                # html.H3('The selected attribute is ( ' + derived_virtual_selected_rows[0] + ' )')
                html.H3(text)
            ], className="six columns")




@app.callback(
    Output('pfds-container', "children"),
    [Input('pfds-table', "derived_virtual_data"),
     Input('pfds-table', "derived_virtual_selected_rows")])
def update_graphs_pfds(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    global gresults, gdf
    # print(derived_virtual_selected_rows)
    if not(derived_virtual_selected_rows):
        derived_virtual_selected_rows = []
        return html.Div([
            html.H3('')
        ], className="six columns")
    else:
        pfds = gresults['pfds']
        # if derived_virtual_selected_rows[0] in gms.keys():
        req_pfd = pfds[derived_virtual_selected_rows[0]]
        data = []
        for ii in range(len(data)):
            data.remove(data[0])
        for tp in req_pfd['tableau']:
            ((a,b), c) = tp
            data.append((a,b,len(c)))
        cols = ['Determinant Pattern', 'Dependent Pattern', '# affected tuples']
        tableau_df = pd.DataFrame(data, columns=cols)
        return html.Div([
            dash_table.DataTable(
            data=tableau_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in tableau_df.columns],
            id='pfds-tableau-table',
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': '600px',
                'textAlign':'left',
                'font-size': '150%',
                'font-family': 'Times New Roman',
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'maxWidth':'800px',
                'overflowY': 'scroll',
                'margin-left': '20px',
                # 'border': 'thin lightgrey solid',
            }
        ),
            # html.H3(gdf.columns[derived_virtual_selected_rows] + '   Just for test')
        ])
        



@app.callback(Output('pfds-container-hidden', 'children'),
              [Input('pfds-table', 'data_previous')],
              [State('pfds-table', 'data')])
def show_removed_rows(previous, current):
    global gresults
    if previous is None:
        dash.exceptions.PreventUpdate()
    else:
        for row in previous:
            if row not in current:
                rem_det = row['Determinant'][0]
                rem_dep = row['Dependent'][0]
                for jj in range(len(gresults['pfds'])):
                    if gresults['pfds'][jj]['det'] == row['Determinant'][0] and gresults['pfds'][jj]['dep'] == row['Dependent'][0]:
                        gresults['pfds'].remove(gresults['pfds'][jj])
                        # print(gresults['pfds'][jj]['det'], gresults['pfds'][jj]['dep'], jj)
                        break
                # print(row['Determinant'][0], '===>', row['Dependent'][0])
        return html.Div([
                # html.H3('The selected attribute is ( ' + derived_virtual_selected_rows[0] + ' )')
                html.H3(""),
            ])






@app.callback(
    Output('vios-container', "children"),
    [Input('vios-table', "derived_virtual_data"),
     Input('vios-table', "derived_virtual_selected_rows")])
def update_graphs_vios(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    global gresults, gdf
    # print(derived_virtual_selected_rows)
    if not(derived_virtual_selected_rows):
        derived_virtual_selected_rows = []
        return html.Div([
            html.H3('')
        ], className="six columns")
    else:
        pfds = gresults['pfds']
        # if derived_virtual_selected_rows[0] in gms.keys():
        req_pfd = pfds[derived_virtual_selected_rows[0]]
        data = []
        for ii in range(len(data)):
            data.remove(data[0])
        
        if len(req_pfd['vios']) > 0:
            vios_df = req_pfd['vios']
            
            
            det_name = ''
            dep_name = ''
            
            for col in vios_df.columns:
                if col == req_pfd['det']:
                    det_name = col
                if col == req_pfd['dep']:
                    dep_name = col
            cols = [det_name, dep_name]
            data = vios_df[cols]
            prjected_vios_df = pd.DataFrame(data, columns=cols)
            return html.Div([
                dash_table.DataTable(
                data=prjected_vios_df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in prjected_vios_df.columns],
                id='vios-values-table',
                row_selectable="single",
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
                }],
                style_cell={
                    'whiteSpace': 'no-wrap',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxWidth': '600px',
                    'textAlign':'left',
                    'font-size': '150%',
                    'font-family': 'Times New Roman',
                },
                style_cell_conditional=[
                    {'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)',},
                    {'if': {'column_id': det_name},
                    'backgroundColor': 'white',
                    'color': '#3D9970',},
                    {'if': {'column_id': dep_name},
                    'backgroundColor': 'white',
                    'color': '#9D3D70',},
                ],
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold',
                    # 'textAlign': 'center'
                },
                style_table={
                    'max_rows_in_viewport':15,
                    'maxHeight': '400px',
                    # 'maxWidth':'800px',
                    'overflowY': 'scroll',
                    'margin-left': '20px',
                    # 'border': 'thin lightgrey solid',
                }
            ),
            html.H3('Select a violation to see its context'),
            html.Hr(),    
            html.Div(id='vios-explain', className="twelve columns"), 
            ])
        else:
            return html.Div([
                # html.H3('The selected attribute is ( ' + derived_virtual_selected_rows[0] + ' )')
                html.H3(""),
            ])
        



@app.callback(
    Output('vios-explain', "children"),
    [Input('vios-values-table', "derived_virtual_data"),
     Input('vios-values-table', "derived_virtual_selected_rows"),
     Input('vios-table', "derived_virtual_data"),
     Input('vios-table', "derived_virtual_selected_rows")])
def update_graphs_vios_w_details(rows_vio, derived_virtual_selected_rows_vio, rows_pfds, derived_virtual_selected_rows_pfd):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    global gresults, gdf
    # print(derived_virtual_selected_rows)
    if not(derived_virtual_selected_rows_pfd):
        derived_virtual_selected_rows_pfd = []
        return html.Div([
            html.H3('')
        ], className="six columns")
    elif not(derived_virtual_selected_rows_vio):
        derived_virtual_selected_rows_vio = []
        return html.Div([
            html.H3('')
        ], className="six columns")
    else:    
        pfds = gresults['pfds']
        comp_data = rows_vio[derived_virtual_selected_rows_vio[0]]
        
        req_pfd = pfds[derived_virtual_selected_rows_pfd[0]]
        if len(req_pfd['vios']) == 0:
            return html.Div([html.H3('')])
        if not(derived_virtual_selected_rows_vio[0] < len(req_pfd['vios'].index.tolist())):
            return html.Div([html.H3('')])
        req_vio = int(req_pfd['vios'].index.tolist()[derived_virtual_selected_rows_vio[0]])
        req_idx = []
        for jj in range(len(req_idx)):
            rec_idx.remove(req_idx[0])
        det_name = ''
        dep_name = ''
        vios_df = req_pfd['vios']
        for col in vios_df.columns:
            if col == req_pfd['det']:
                det_name = col
            if col == req_pfd['dep']:
                dep_name = col
        rule = None
        for tp in req_pfd['tableau']:
            ((a, b), c) = tp
            rule = (a,b)
            if req_vio in c:
                req_idx = c
                break
        req_vio_df = gdf.loc[req_idx]
        if not (det_name in comp_data.keys()) or not (dep_name in comp_data.keys()):
            return html.Div([html.H3("")])
        # req_vio_rec = pd.DataFrame(req_pfd['vios'].loc[req_vio])
        # print (req_pfd['vios'].index.tolist(), req_vio)
        mylist = []
        for kk in range(len(mylist)):
            mylist.remove(mylist[0])
        req_vio_df_idx = req_vio_df.index.tolist()
        for kk in range(len(req_vio_df_idx)):
            jj = req_vio_df_idx[kk]
            if (req_vio_df[det_name][jj] == comp_data[det_name]) and (req_vio_df[dep_name][jj] == comp_data[dep_name]):
                mylist.append(kk)
        aa = ''
        bb = ''
        if rule:
            (aa, bb) = rule
        
        return html.Div([
                html.H3("Violation(s) in context:  [ " + aa + '  ==>  ' + bb + ' ]'),
                dash_table.DataTable(
                data=req_vio_df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in req_vio_df.columns],
                # id='vios-explain-table',
                # row_selectable="single",
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
                }],
                style_cell={
                    'whiteSpace': 'no-wrap',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxWidth': '600px',
                    'textAlign':'left',
                    'font-size': '150%',
                    'font-family': 'Times New Roman',
                },
                style_cell_conditional=[
                    {
                        'if': {'row_index': x,
                            'column_id': det_name,
                        },
                        'backgroundColor': 'lightblue',} for x in mylist
                ]+[
                    {
                        'if': {'row_index': x,
                            'column_id': dep_name,
                        },
                        'backgroundColor': 'lightblue',} for x in mylist
                ]+[
                    {'if': {'column_id': det_name,},
                    'color': '#3D9970',},
                    {'if': {'column_id': dep_name,},
                    'color': '#9D3D70',},
                ],
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold'
                },
                style_table={
                    'max_rows_in_viewport':15,
                    'maxHeight': '400px',
                    # 'maxWidth':'1000px',
                    'overflowY': 'scroll',
                    'margin-left': '20px',
                    # 'border': 'thin lightgrey solid',
                }
            ), 
            ])
        # else:
        #     return html.Div([
        #         # html.H3('The selected attribute is ( ' + derived_virtual_selected_rows[0] + ' )')
        #         html.H3(""),
        #     ])







# # # # # # # # #
# detail the way that external_css and external_js work and link to alternative method locally hosted
# # # # # # # # #
external_css = ["https://cdnjs.cloudflare.com/ajax/libs/normalize/7.0.0/normalize.min.css",
                "https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                "//fonts.googleapis.com/css?family=Raleway:400,300,600",
                "https://codepen.io/bcd/pen/KQrXdb.css",
                "https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css",
                "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css",
                "https://www.w3schools.com/w3css/4/w3.css",
                "/assets/style.css"]

for css in external_css:
    app.css.append_css({"external_url": css})

external_js = ["https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js",
               "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js",
               ]

for js in external_js:
    app.scripts.append_script({"external_url": js})






if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')



import base64
import datetime
import io
import os 
from os import listdir
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import ntpath
import csv
import pandas as pd
# from extra import find_pfds_csv
from pfd import find_pfds_csv

from components import Header, make_dash_table, get_menu

DATA_FOLDER = "./data"


# important link https://dash.plot.ly/datatable/interactivity
# external CSS stylesheets
external_stylesheets = [
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    }
]



app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.config['suppress_callback_exceptions']=True
app.title = 'ANMAT'

DATA_FOLDER = "./data"
gdf = None
gresults = None
param_dict = dict()
param_dict["results_main_dir"] = "../Results/"




def read_table(tab_name):
    t_name = ntpath.basename(tab_name)
    try:
        df = pd.read_csv(filepath_or_buffer=tab_name, dtype=object, delimiter=',', low_memory=False,
                         quoting=csv.QUOTE_ALL, doublequote=True)
    except ValueError:
        try:
            df = pd.read_csv(filepath_or_buffer=tab_name, dtype=object, delimiter=',', low_memory=False,
                             quoting=csv.QUOTE_ALL, doublequote=True, encoding="ISO-8859-1")
        except:
            print("Error reading csv file .. file encoding is not recognizable")
            return None
    return df

def get_csv_files(folder):
    csv_files = []
    folder_contents = listdir(folder)
    for item in folder_contents:
        if item.endswith('.csv'):
            csv_files.append(item)
    return csv_files


def dynamic_page():
    return html.Div([
        Header(),
        html.Div([    
            html.Div([
                dcc.Dropdown(
                        id='uploaded-datasets',
                        options=[{'label': i, 'value': i} for i in get_csv_files(DATA_FOLDER)],
                        placeholder='Select a Dataset',
                    )
                ],style={
                        'width': '220px',
                        'display': 'inline-block',
                        'margin-left': '25px',
                    } 
                
            ), 
            html.Div([
                dcc.Upload(
                    id='upload-data',
                    children = html.Div([
                        html.Button(' Browse ', className='fa fa-upload')
                    ],style={
                        'backgroundColor':'green',
                        'color':'white',
                        'margin-left': '5px',
                    }),
                    
                    multiple=False
                ),
            ]),
            html.Div([
                dcc.Input(
                    placeholder='Enter the Confidence',
                    type='text',
                    value='',
                    id='Confidence'
                )
                ],style={
                        'width': '200',
                        'display': 'inline-block',
                        'margin-left': '5px',
                    }),
            html.Div([
                dcc.Input(
                    placeholder='Enter the Allowed Violations',
                    type='text',
                    value='',
                    id='Delta'
                )
                ],style={
                        'width': '200',
                        'display': 'inline-block',
                        'margin-left': '5px',
                    }),
            html.Div([
                dcc.Input(
                    placeholder='Enter the Min. Coverage',
                    type='text',
                    value='',
                    id='Coverage'
                )
                ],style={
                        'width': '200px',
                        'display': 'inline-block',
                        'margin-left': '5px',
                    }),
            html.Button('PFD Discovery', className='fa', id='button', 
                style={
                        'backgroundColor':'green',
                        'color':'white',
                        'width': '200',
                        'flow':'right',
                        'margin-left': '15px',
                    }),
        ], className="row",
            style={
                'width': '100%',
                'height':'50px',
                'borderWidth': '1px',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin-left': '25px',
                'margin-top': '10px',
            }),
        html.Div(id='output-data-upload'),
        html.Div(id='output-data-dropdown',
            style={
                'width': '100%',
                'height': '440px',
                'borderWidth': '1px',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin-left': '50px',
                'margin-right': '25px',
                'overflowY': 'scroll',
            }),
        html.Hr(),  # horizontal line
        html.Div(id = 'output-results',
            style={
                'width': '100%',
                'height': '200px',
                'borderWidth': '1px',
                'borderRadius': '5px',
                'textAlign': 'left',
                'margin-left': '25px',
                'margin-right': '25px',
                'margin-top': '40px'
            }),
        # Footer()
    ], className='body')

app.layout = dynamic_page



def upload_contents(contents, filename):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    new_file_name = ''
    try:
        if filename.endswith('csv'):
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
            if filename.endswith('xls'):
                new_file_name = filename.replace('.xls', '.csv')
            if filename.endswith('xlsx'):
                new_file_name = filename.replace('.xlsx', '.csv')
        filename2 = os.path.join(DATA_FOLDER, filename)
        df.to_csv(filename2, sep=',', encoding='latin', index=False, quoting=csv.QUOTE_ALL, doublequote=True)
    except Exception as e:
        print(e)
        return html.Div([
            ''
        ])
    return html.Div([
            ''
        ])


def parse_contents(filename):
    global gdf
    try:
        if filename.endswith('csv'):
            # Assume that the user uploaded a CSV file
            filename2 = os.path.join(DATA_FOLDER, filename)
            df = read_table(filename2)
            gdf = df
        # elif 'xls' in filename:
        #     # Assume that the user uploaded an excel file
        #     df = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])

    return html.Div([
        dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0,
                'textAlign':'left'
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'overflowY': 'scroll'
            },
            pagination_settings={
            "current_page": 0,
            "page_size": 50,
            },
        ),
        html.Hr(),  # horizontal line
    ], className='ui-grid-resize-columns ui-grid-pagination')


@app.callback([Output('output-data-upload', 'children'),
                Output('uploaded-datasets', 'options'),
                Output('uploaded-datasets', 'value')],
              [Input('upload-data', 'contents')],
              [State('upload-data', 'filename'),
               State('upload-data', 'last_modified')])
def update_output(content, fname, modified):
    if content:
        grid = upload_contents(content, fname)
        options=[{'label': i, 'value': i} for i in get_csv_files(DATA_FOLDER)]
        value=fname
        return grid, options, value
    else:
        options=[{'label': i, 'value': i} for i in get_csv_files(DATA_FOLDER)]
        return html.Div(['']), options, ''


@app.callback(Output('output-data-dropdown', 'children'),
              [Input('uploaded-datasets', 'value')])
def output_dropdown(fname):
    options=[{'label': i, 'value': i} for i in get_csv_files(DATA_FOLDER)]
    if fname:
        grid = parse_contents(fname)
        return grid
    else:
        return html.Div([''])
        
@app.callback(
    Output('output-results', 'children'),
    [Input('button', 'n_clicks')],
    [State('uploaded-datasets', 'value'),
    State('Confidence', 'value'),
    State('Delta', 'value'),
    State('Coverage', 'value')])
def update_output(n_clicks, fname, conf, delta, min_coverage):
    
    global gresults

    if fname:
        if conf:
            if delta:
                if min_coverage:
                    param_dict["tab_name"] = os.path.join(DATA_FOLDER, fname)
                    param_dict["min_acceptable_coverage"] = float(min_coverage) / 100.0 
                    param_dict["confidence_K"] = float(conf)
                    param_dict["allowed_noise_delta"] = float(delta)
                    gresults = find_pfds_csv(param_dict)
                    
                    return html.Div([
                        dcc.Tabs(
                            id="tabs-with-classes",
                            value='patt',
                            parent_className='custom-tabs',
                            className='tab',
                            children=[
                                dcc.Tab(
                                    label='Patterns',
                                    value='patt',
                                    className='custom-tab',
                                    selected_className='custom-tab--selected'
                                ),
                                dcc.Tab(
                                    label='PFDs',
                                    value='pfds',
                                    className='custom-tab',
                                    selected_className='custom-tab--selected'
                                ),
                                dcc.Tab(
                                    label='Violations',
                                    value='vio', className='custom-tab',
                                    selected_className='custom-tab--selected'
                                ),
                            ]),
                        html.Div(id='tabs-content-classes')
                    ])
                else:
                    return html.Div(['The min_coverage is missing'])
            else:
                return html.Div(['The allowed violations is missing'])
        else:
            html.Div(['The confidence is missing'])
    else:
        html.Div(['The data file is missing'])
    if n_clicks:
        return html.Div(['Something goes wrong after {}'.format(n_clicks)])
    return html.Div([''])





@app.callback(Output('tabs-content-classes', 'children'),
              [Input('tabs-with-classes', 'value')],
              [State('uploaded-datasets', 'value')])
def render_content(tab, tab_name):
    global gdf, gresults
    
    if tab == 'patt':
        att_names = gdf.columns.tolist()
        if gresults:
            df_details = gresults['df_details']
        tok_or_ngrams = dict()
        tok_or_ngrams.clear()
        for d in df_details.keys():
            tok_or_ngrams[df_details[d]['att_name']] = df_details[d]['tg_vs_ng']
        data = []
        cols = ['Attributes', 'Tokens or n-Grams']
        for i in range(len(data)):
            data.remove(data[0])
        gms = gresults['patterns']
        for k in tok_or_ngrams.keys():
            new_k = gdf.columns.get_loc(k)
            # print(new_k, gms.keys())
            if new_k in gms.keys():
                data.append([k, tok_or_ngrams[k]])
            else:
                data.append([k, '----'])
        att_df = pd.DataFrame(data, columns=cols)
        return html.Div([
            dash_table.DataTable(
            data=att_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in att_df.columns],
            id='patterns-table',
            row_selectable="single",
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': '500px',
                'textAlign':'left',
                'font-size': '150%',
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'maxWidth':'600px',
                'overflowY': 'scroll',
                'margin-left': '20px',
                # 'border': 'thin lightgrey solid',
            }
        ),
            # html.H3('Select an attribute to see the patterns extracted from the attribute.'),
            html.Div(id='patterns-container', className="six columns"), 
        ], className="row ")

    elif tab == 'pfds':
        # att_names = gdf.columns.tolist()
        if gresults:
            pfds = gresults['pfds']
        data = []
        cols = ['Determinant', 'Dependent']
        for i in range(len(data)):
            data.remove(data[0])
        for pfd in pfds:
            data.append([pfd['det'], pfd['dep']])
        pfds_df = pd.DataFrame(data, columns=cols)
        return html.Div([
            dash_table.DataTable(
            data=pfds_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in pfds_df.columns],
            id='pfds-table',
            row_selectable="single",
            row_deletable=True,
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': '500px',
                'textAlign':'left',
                'font-size': '150%',
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'maxWidth':'600px',
                'overflowY': 'scroll',
                'margin-left': '20px',
                # 'border': 'thin lightgrey solid',
            }
        ),
            # html.H3('Select a dependency to see its tableau of PFDs'),
            html.Div(id='pfds-container', className="six columns"),
            html.Div(id='pfds-container-hidden', style={'display':'none'}),
        ], className="row ")

    elif tab == 'vio':
        if gresults:
            pfds = gresults['pfds']
        data = []
        cols = ['Determinant', 'Dependent']
        for i in range(len(data)):
            data.remove(data[0])
        for pfd in pfds:
            data.append([pfd['det'], pfd['dep']])
        pfds_df = pd.DataFrame(data, columns=cols)
        return html.Div([
            dash_table.DataTable(
            data=pfds_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in pfds_df.columns],
            id='vios-table',
            row_selectable="single",
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': '500px',
                'textAlign':'left',
                'font-family': 'Times New Roman',
                'font-size': '150%',
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'maxWidth':'600px',
                'overflowY': 'scroll',
                'margin-left': '20px',
                # 'border': 'thin lightgrey solid',
            }
        ),
            html.Div(id='vios-container', className="six columns"), 
        ], className="row ")


@app.callback(
    Output('patterns-container', "children"),
    [Input('patterns-table', "derived_virtual_data"),
     Input('patterns-table', "derived_virtual_selected_rows")])
def update_graphs(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    global gresults, gdf
    # print(derived_virtual_selected_rows)
    if not(derived_virtual_selected_rows):
        derived_virtual_selected_rows = []
        return html.Div([
            html.H4('') 
        ], className="six columns")
    else:
        gms = gresults['patterns']
        if derived_virtual_selected_rows[0] in gms.keys():
            req_gms = gms[derived_virtual_selected_rows[0]]
            patt_df = pd.DataFrame(req_gms, columns=['patterns', 'frequency'])
            return html.Div([
                dash_table.DataTable(
                data=patt_df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in patt_df.columns],
                id='patterns-freq-table',
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
                }],
                style_cell={
                    'whiteSpace': 'no-wrap',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxWidth': '600px',
                    'textAlign':'left',
                    'font-size': '150%',
                    'font-family': 'Times New Roman'
                },
                style_cell_conditional=[{
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)'
                }],
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold'
                },
                style_table={
                    'max_rows_in_viewport':15,
                    'maxHeight': '400px',
                    'maxWidth':'800px',
                    'overflowY': 'scroll',
                    'margin-left': '20px',
                    # 'border': 'thin lightgrey solid',
                }
            ),
                # html.H3(gdf.columns[derived_virtual_selected_rows] + '   Just for test')
            ])
        else:
            text = '(' + gdf.columns[derived_virtual_selected_rows[0]] + ') has been ignored because it represents ' 
            text += 'a numerical quantity '
            return html.Div([
                # html.H3('The selected attribute is ( ' + derived_virtual_selected_rows[0] + ' )')
                html.H3(text)
            ], className="six columns")




@app.callback(
    Output('pfds-container', "children"),
    [Input('pfds-table', "derived_virtual_data"),
     Input('pfds-table', "derived_virtual_selected_rows")])
def update_graphs(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    global gresults, gdf
    # print(derived_virtual_selected_rows)
    if not(derived_virtual_selected_rows):
        derived_virtual_selected_rows = []
        return html.Div([
            html.H3('')
        ], className="six columns")
    else:
        pfds = gresults['pfds']
        # if derived_virtual_selected_rows[0] in gms.keys():
        req_pfd = pfds[derived_virtual_selected_rows[0]]
        data = []
        for ii in range(len(data)):
            data.remove(data[0])
        for tp in req_pfd['tableau']:
            ((a,b), c) = tp
            data.append((a,b,len(c)))
        cols = ['Determinant Pattern', 'Dependent Pattern', '# affected tuples']
        tableau_df = pd.DataFrame(data, columns=cols)
        return html.Div([
            dash_table.DataTable(
            data=tableau_df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in tableau_df.columns],
            id='pfds-tableau-table',
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
            }],
            style_cell={
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': '600px',
                'textAlign':'left',
                'font-size': '150%',
                'font-family': 'Times New Roman',
            },
            style_cell_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }],
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_table={
                'max_rows_in_viewport':15,
                'maxHeight': '400px',
                'maxWidth':'800px',
                'overflowY': 'scroll',
                'margin-left': '20px',
                # 'border': 'thin lightgrey solid',
            }
        ),
            # html.H3(gdf.columns[derived_virtual_selected_rows] + '   Just for test')
        ])
        



@app.callback(Output('pfds-container-hidden', 'children'),
              [Input('pfds-table', 'data_previous')],
              [State('pfds-table', 'data')])
def show_removed_rows(previous, current):
    global gresults
    if previous is None:
        dash.exceptions.PreventUpdate()
    else:
        for row in previous:
            if row not in current:
                rem_det = row['Determinant'][0]
                rem_dep = row['Dependent'][0]
                for jj in range(len(gresults['pfds'])):
                    if gresults['pfds'][jj]['det'] == row['Determinant'][0] and gresults['pfds'][jj]['dep'] == row['Dependent'][0]:
                        gresults['pfds'].remove(gresults['pfds'][jj])
                        # print(gresults['pfds'][jj]['det'], gresults['pfds'][jj]['dep'], jj)
                        break
                # print(row['Determinant'][0], '===>', row['Dependent'][0])
        return html.Div([
                # html.H3('The selected attribute is ( ' + derived_virtual_selected_rows[0] + ' )')
                html.H3(""),
            ])






@app.callback(
    Output('vios-container', "children"),
    [Input('vios-table', "derived_virtual_data"),
     Input('vios-table', "derived_virtual_selected_rows")])
def update_graphs(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    global gresults, gdf
    # print(derived_virtual_selected_rows)
    if not(derived_virtual_selected_rows):
        derived_virtual_selected_rows = []
        return html.Div([
            html.H3('')
        ], className="six columns")
    else:
        pfds = gresults['pfds']
        # if derived_virtual_selected_rows[0] in gms.keys():
        req_pfd = pfds[derived_virtual_selected_rows[0]]
        data = []
        for ii in range(len(data)):
            data.remove(data[0])
        
        if len(req_pfd['vios']) > 0:
            vios_df = req_pfd['vios']
            
            
            det_name = ''
            dep_name = ''
            
            for col in vios_df.columns:
                if col == req_pfd['det']:
                    det_name = col
                if col == req_pfd['dep']:
                    dep_name = col
            cols = [det_name, dep_name]
            data = vios_df[cols]
            prjected_vios_df = pd.DataFrame(data, columns=cols)
            return html.Div([
                dash_table.DataTable(
                data=prjected_vios_df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in prjected_vios_df.columns],
                id='vios-values-table',
                row_selectable="single",
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
                }],
                style_cell={
                    'whiteSpace': 'no-wrap',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxWidth': '600px',
                    'textAlign':'left',
                    'font-size': '150%',
                    'font-family': 'Times New Roman',
                },
                style_cell_conditional=[
                    {'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)',},
                    {'if': {'column_id': det_name},
                    'backgroundColor': 'white',
                    'color': '#3D9970',},
                    {'if': {'column_id': dep_name},
                    'backgroundColor': 'white',
                    'color': '#9D3D70',},
                ],
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold',
                    # 'textAlign': 'center'
                },
                style_table={
                    'max_rows_in_viewport':15,
                    'maxHeight': '400px',
                    # 'maxWidth':'800px',
                    'overflowY': 'scroll',
                    'margin-left': '20px',
                    # 'border': 'thin lightgrey solid',
                }
            ),
            html.H3('Select a violation to see its context'),
            html.Hr(),    
            html.Div(id='vios-explain', className="twelve columns"), 
            ])
        else:
            return html.Div([
                # html.H3('The selected attribute is ( ' + derived_virtual_selected_rows[0] + ' )')
                html.H3(""),
            ])
        



@app.callback(
    Output('vios-explain', "children"),
    [Input('vios-values-table', "derived_virtual_data"),
     Input('vios-values-table', "derived_virtual_selected_rows"),
     Input('vios-table', "derived_virtual_data"),
     Input('vios-table', "derived_virtual_selected_rows")])
def update_graphs(rows_vio, derived_virtual_selected_rows_vio, rows_pfds, derived_virtual_selected_rows_pfd):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    global gresults, gdf
    # print(derived_virtual_selected_rows)
    if not(derived_virtual_selected_rows_pfd):
        derived_virtual_selected_rows_pfd = []
        return html.Div([
            html.H3('')
        ], className="six columns")
    elif not(derived_virtual_selected_rows_vio):
        derived_virtual_selected_rows_vio = []
        return html.Div([
            html.H3('')
        ], className="six columns")
    else:    
        pfds = gresults['pfds']
        comp_data = rows_vio[derived_virtual_selected_rows_vio[0]]
        # if derived_virtual_selected_rows[0] in gms.keys():
        req_pfd = pfds[derived_virtual_selected_rows_pfd[0]]
        if len(req_pfd['vios']) == 0:
            return html.Div([html.H3('')])
        req_vio = int(req_pfd['vios'].index.tolist()[derived_virtual_selected_rows_vio[0]])
        req_idx = []
        for jj in range(len(req_idx)):
            rec_idx.remove(req_idx[0])
        det_name = ''
        dep_name = ''
        vios_df = req_pfd['vios']
        for col in vios_df.columns:
            if col == req_pfd['det']:
                det_name = col
            if col == req_pfd['dep']:
                dep_name = col
        rule = None
        for tp in req_pfd['tableau']:
            ((a, b), c) = tp
            rule = (a,b)
            if req_vio in c:
                req_idx = c
                break
        req_vio_df = gdf.loc[req_idx]
        # req_vio_rec = pd.DataFrame(req_pfd['vios'].loc[req_vio])
        # print (req_pfd['vios'].index.tolist(), req_vio)
        mylist = []
        for kk in range(len(mylist)):
            mylist.remove(mylist[0])
        req_vio_df_idx = req_vio_df.index.tolist()
        for kk in range(len(req_vio_df_idx)):
            jj = req_vio_df_idx[kk]
            if (req_vio_df[det_name][jj] == comp_data[det_name]) and (req_vio_df[dep_name][jj] == comp_data[dep_name]):
                mylist.append(kk)
        aa = ''
        bb = ''
        if rule:
            (aa, bb) = rule
        
        return html.Div([
                html.H3("Violation(s) in context:  [ " + aa + '  ==>  ' + bb + ' ]'),
                dash_table.DataTable(
                data=req_vio_df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in req_vio_df.columns],
                # id='vios-explain-table',
                # row_selectable="single",
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; margin-left: 20px; overflow: inherit; text-overflow: inherit;'
                }],
                style_cell={
                    'whiteSpace': 'no-wrap',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxWidth': '600px',
                    'textAlign':'left',
                    'font-size': '150%',
                    'font-family': 'Times New Roman',
                },
                style_cell_conditional=[
                    {
                        'if': {'row_index': x,
                            'column_id': det_name,
                        },
                        'backgroundColor': 'lightblue',} for x in mylist
                ]+[
                    {
                        'if': {'row_index': x,
                            'column_id': dep_name,
                        },
                        'backgroundColor': 'lightblue',} for x in mylist
                ]+[
                    {'if': {'column_id': det_name,},
                    'color': '#3D9970',},
                    {'if': {'column_id': dep_name,},
                    'color': '#9D3D70',},
                ],
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold'
                },
                style_table={
                    'max_rows_in_viewport':15,
                    'maxHeight': '400px',
                    # 'maxWidth':'1000px',
                    'overflowY': 'scroll',
                    'margin-left': '20px',
                    # 'border': 'thin lightgrey solid',
                }
            ), 
            ])
        # else:
        #     return html.Div([
        #         # html.H3('The selected attribute is ( ' + derived_virtual_selected_rows[0] + ' )')
        #         html.H3(""),
        #     ])







# # # # # # # # #
# detail the way that external_css and external_js work and link to alternative method locally hosted
# # # # # # # # #
external_css = ["https://cdnjs.cloudflare.com/ajax/libs/normalize/7.0.0/normalize.min.css",
                "https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                "//fonts.googleapis.com/css?family=Raleway:400,300,600",
                "https://codepen.io/bcd/pen/KQrXdb.css",
                "https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css",
                "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css",
                "https://www.w3schools.com/w3css/4/w3.css",
                "/assets/style.css"]

for css in external_css:
    app.css.append_css({"external_url": css})

external_js = ["https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js",
               "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js",
               ]

for js in external_js:
    app.scripts.append_script({"external_url": js})






if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')




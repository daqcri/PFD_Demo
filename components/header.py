import dash_html_components as html
import dash_core_components as dcc

def Header():
    return html.Div([
        get_logo()
    ])

def get_logo():
    logo = html.Div([

        html.Div([
            html.Img(src='/assets/Logo_red.png', id='logo', height='120', width='165')
        ], style={
                    'width': '15%',
                    'display': 'inline-block',
                    'align':'middle'

                }, className="w3-bar-item w3-button w3-white"),
        html.Div([
            html.H1('ANMAT: Automatic Knowledge Discovery and Error Detection'),
            html.H1('through Pattern Functional Dependencies')
        ])

    ],className="w3-bar w3-blue")


    # ], className="row gs-header")
    return logo

def Footer():
    # return html.Div([
    #         html.Img(src='/assets/Logo_red.png', id='heart', height='120', width='165')
    #     ], style={
    #                 'width': '15%',
    #                 'display': 'inline-block',
    #                 'align':'middle'

    #             }, className="w3-bar-item w3-button w3-white"),
        html.Div([
            html.A('QCRI-DA', href='http://da.qcri.org')
        ])
def make_dash_table(df):
    ''' Return a dash definition of an HTML table for a Pandas dataframe '''
    table = []
    for index, row in df.iterrows():
        html_row = []
        for i in range(len(row)):
            html_row.append(html.Td([row[i]]))
        table.append(html.Tr(html_row))
    return table


def get_menu():
    menu = html.Div([

        dcc.Link('Patterns   ', href='/patterns', className="tab first"),

        dcc.Link('PFDs   ', href='/pfds', className="tab"),

        dcc.Link('Violations   ', href='/violations', className="tab"),

        # dcc.Link('Fees & Minimums   ', href='/fees', className="tab"),

        # dcc.Link('Distributions   ', href='/distributions', className="tab"),

        # dcc.Link('News & Reviews   ', href='/news-and-reviews', className="tab")

    ], className="row ")
    return menu
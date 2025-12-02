import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from Utils import logger
import logging

logger = logging.getLogger(__name__)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

app.layout = dbc.Container([
    html.Div(# left section / navigation section
            [html.Div([html.H1("GTC Analytics Dashboard"),
                       html.Br(),
                       html.Span("Navigation Menu")],
                    style={'width': '100%', 'height': '282px', 'margin-top': '0px', 'backgroundColor': "#1D66D4"}, className='header-section'), 
             html.Div([html.Div(), html.Div(), html.Div(), html.Div()], 
                     style={'width': '100%', 'height': '65px', 'margin-top': '20px', 'margin-bottom': '20px', 'backgroundColor': "#C51DD4"}, className='header-section'),
              html.Div(style={'width': '100%', 'height': '70px', 'backgroundColor': "#1DD44B"}, className='header-section'),
              html.Div(style={'width': '100%', 'height': '70px','margin-top': '5px', 'backgroundColor': "#D1D41D"}, className='header-section'),
              html.Div(style={'width': '100%', 'height': '70px','margin-top': '5px', 'backgroundColor': "#C51DD4"}, className='header-section'),
              html.Div(style={'width': '100%', 'height': '70px','margin-top': '5px', 'backgroundColor': "#4B1DD4"}, className='header-section'),
              html.Div(style={'width': '100%', 'height': '100px','margin-top': '20px', 'margin-bottom': '0px', 'backgroundColor': "#D41D26"}, className='header-section')],
            className='navigation-panel',
             style={
                 'width': '25%',
                 'margin-left': 35,
                 'margin-top': 35,
                 'margin-bottom': 35,
                 'margin-right': 2.5,
             }),
    html.Div( # center section
        [html.Div(style={'width': '79%', 'backgroundColor': "#E71F1F"}),
         html.Div(style={'width': '21%', 'backgroundColor': "#7C2323"})],className='test-box',
        style={
            'width': '75%',
            'margin-top': 35,
            'margin-right': 35,
            'margin-bottom': 35,
            'margin-left': 2.5,
            'display': 'flex',
        })
],
    fluid=True,
    style={'display': 'flex'},
    className='dashboard-container')







# dbc.Container([
#         html.Div(style={'backgroundColor': "#656666", 'borderRadius': '10px', 'margin': '10px'}, children=[
#                 html.H1("GTC Analytics Dashboard", 
#                         style={'textAlign': 'center', 'color': "#CACACA", 'padding': '20px'}),
#                 html.Hr(style={'borderWidth': "5px", 'borderColor': "#CACACA"},),
#                 html.H2("Dashboard Content", 
#                         style={'textAlign': 'center', 'color': "#CACACA", 'padding': '15px'}),
#                         ], 
#                  className='header-section',
#                  style={'flex': '1',
#                         'width': '25%'
#                         }
#                  ),
#         html.Div(style={'backgroundColor': "#656666", 'borderRadius': '10px', 'margin': '10px'}, children=[
#                 html.H2("Additional Section", 
#                         style={'textAlign': 'center', 'color': "#CACACA", 'padding': '15px'}),
#         ], className='header-section'),
# ], fluid=True, className='dashboard-container', style={'display': 'flex'})

        # html.H1("GTC Analytics Dashboard", 
        #         style={'textAlign': 'center', 'color': "#CACACA", 'padding': '20px'},),
        # html.Hr(style={'borderWidth': "10px", 'borderColor': "#CACACA"},),
        # html.H2("Dashboard Content", 
        #         style={'textAlign': 'center', 'color': "#CACACA", 'padding': '15px'}),
        
        

# ])

if __name__ == '__main__':
    try:
        logger.info("Starting Dashboard Application")
        app.run(debug=True)
    except Exception as e:
        logger.error(f"Error starting Dashboard Application: {e}")
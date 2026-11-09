"""GTC Analytics Dashboard (layout prototype).   gtc-dashboard  ->  http://127.0.0.1:8050

Status: layout only. The coloured boxes are placeholders; charts from
figure.py and data from data.py are not wired in yet. See dashboard-brief.md.
"""
import logging

import dash
import dash_bootstrap_components as dbc
from dash import html

from gtc_common.log import setup_logging

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


def main() -> None:
    setup_logging("dashboard")
    logger.info("Starting dashboard on http://127.0.0.1:8050")
    app.run(debug=True)


if __name__ == "__main__":
    main()

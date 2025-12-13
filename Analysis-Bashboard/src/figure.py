import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import folium


import logging
from Utils import logger
logger = logging.getLogger(__name__)
logger.info("figure module loaded")

# temp section used for testing plots with real data
import os
from dotenv import load_dotenv
load_dotenv()

data = pd.read_csv(os.getenv('lineitem_analysis_test'))
columns = [i for i in data.columns]

#set df view length for testing
pd.set_option('display.max_rows', 5000)

#----------------------------------------#
# FIGURE CREATION FUNCTIONS
#----------------------------------------#


#----------------------------------------#
# LINE PLOT FIGURE
#----------------------------------------#


# this is working as intended
def create_total_profit_over_time(df: pd.DataFrame, title: str) -> go.Figure:
    
    '''
    explanation:
    This function creates a line plot showing profit over time. 
    
    input:
    dataframe taken from a sql query that will have the ability to be filtered by various factors.
    
    output:
    plotly figure object
    
    MANDATORY COLUMNS IN DATAFRAME:
    'OrderDate'
    'Rolling Total'
    
    additions to make:
    add a section that allows for a slider to change the time frame being viewed.
    '''
    
    line_color = '#00CC96'  # green color for profit line
    
    logger.info('Creating profit over time figure')
    
    fig = px.line(
        df, 
        x='OrderDate', 
        y='Rolling Total', 
        title=title
    )
    fig.update_layout(
        template='plotly_dark',
        title={'x':0.5, 'xanchor': 'center', 'yanchor': 'top'}, 
        title_font=dict(size=24, color='#FFFFFF'),
        hoverlabel=dict(
            bgcolor=line_color,
            bordercolor="#141414",
            font=dict(
                color = "#141414",
                size=12
            )
        )
    )
    fig.update_traces(
        line=dict(color=line_color, width=4)
    )
    fig.update_xaxes(
        title_text='Order Date', 
        showgrid=True, 
        gridwidth=1, 
        gridcolor='#444444'
    )
    fig.update_yaxes(
        title_text='Total Profit', 
        showgrid=True, gridwidth=1, 
        gridcolor='#444444',
        tickprefix="$"
    )
    return fig

create_total_profit_over_time(data, 'Total Profit Over Time')  # temp call for testing purposes




data.head()

#----------------------------------------#
# HBAR CHART FIGURE
#----------------------------------------#

def create_hbar_chart(df: pd.DataFrame, title: str, groupby: str, count: str, limit: int) -> go.Figure:
    '''
    explanation:
    This function creates a horizontal bar chart.
    
    input:
    dataframe taken from a sql query that will have the ability to be filtered by various factors.
    
    output:
    plotly figure object
    
    MANDATORY COLUMNS IN QUERY:
    groupby: What ever column is supposed  to be the category axis
    count: What ever column is supposed to be the value axis
    
    additions to make:
    nameing conventions for axes
    '''
    
    logger.info('Creating horizontal bar chart figure')
    
    main_color = '#00CC96'  # green color for profit line

    df1 = df.groupby(groupby)[count].sum().reset_index().sort_values(by=count, ascending=False).tail(limit)

    fig = px.bar(
        df1, 
        x=count, 
        y=groupby, 
        title=title, 
        orientation='h'
    )
    fig.update_layout(
        template='plotly_dark',
        title={'x':0.5, 'xanchor': 'center', 'yanchor': 'top'}, 
        title_font=dict(size=24, color='#FFFFFF'),
        hoverlabel=dict(
            bgcolor=main_color,
            bordercolor="#141414",
            font=dict(
                color = "#141414",
                size=12
            )
        )
    )
    fig.update_traces(
        marker_color=main_color
    )
    fig.update_xaxes(
        title_text=count, 
        showgrid=True, 
        gridwidth=1, 
        gridcolor='#444444'
    )
    fig.update_yaxes(
        title_text=groupby, 
        showgrid=True, 
        gridwidth=1, 
        gridcolor='#444444'
    )
    return fig

create_hbar_chart(data, 'Total Profit by Source', 'Organization Name', 'money_value', 10)  # temp call for testing purposes


#----------------------------------------#
# VBAR CHART FIGURE
#----------------------------------------#


def create_vbar_chart(df: pd.DataFrame, title: str, groupby: str, count: str, limit: int) -> go.Figure:
    '''
    explanation:
    This function creates a vertical bar chart.
    
    input:
    dataframe taken from a sql query that will have the ability to be filtered by various factors.
    
    output:
    plotly figure object
    
    MANDATORY COLUMNS IN QUERY:
    groupby: What ever column is supposed  to be the category axis
    count: What ever column is supposed to be the value axis
    
    additions to make:
    nameing conventions for axes
    '''
    
    logger.info('Creating vertical bar chart figure')

    main_color = '#00CC96'  # green color for profit line

    df1 = df.groupby(groupby)[count].sum().reset_index().sort_values(by=count, ascending=False).head(limit)

    fig = px.bar(
        df1, 
        x=groupby, 
        y=count, 
        title=title
    )
    fig.update_layout(
        template='plotly_dark',
        title={'x':0.5, 'xanchor': 'center', 'yanchor': 'top'}, 
        title_font=dict(size=24, color='#FFFFFF'),
        hoverlabel=dict(
            bgcolor=main_color,
            bordercolor="#141414",
            font=dict(
                color = "#141414",
                size=12
            )
        )
    )
    fig.update_traces(
        marker_color=main_color
    )
    fig.update_xaxes(
        title_text=groupby, 
        showgrid=True, 
        gridwidth=1, 
        gridcolor='#444444'
    )
    fig.update_yaxes(
        title_text=count, 
        showgrid=True, 
        gridwidth=1, 
        gridcolor='#444444'
    )
    return fig
create_vbar_chart(data, 'Total Profit by Organization top ten', 'Organization Name', 'ItemQuantity', 10)  # temp call for testing purposes


#----------------------------------------#
# PIE CHART FIGURE
#----------------------------------------#


def create_pie_chart(df: pd.DataFrame, title: str, names: str, values: str, limit: int) -> go.Figure:
    '''
    explanation:
    This function creates a pie chart.
    
    input:
    dataframe taken from a sql query that will have the ability to be filtered by various factors.
    
    output:
    plotly figure object
    
    MANDATORY COLUMNS IN QUERY:
    names: What ever column is supposed  to be the category axis
    values: What ever column is supposed to be the value axis
    
    additions to make:
    change color scheme
    3D pie chart
    have percentage display outside of pie slices
    '''
    
    logger.info('Creating pie chart figure')

    df1 = df.groupby(names)[values].sum().reset_index().sort_values(by=values, ascending=False).head(limit)

    fig = px.pie(
        df1, 
        names=names, 
        values=values, 
        title=title,
        hole=0.05
    )
    fig.update_layout(
        template='plotly_dark',
        title=dict(
            text=title,
            x=0.5,
            xanchor='center',
            yanchor='top',
        ),
        title_font=dict(size=24, color='#FFFFFF'),
        hoverlabel=dict(
            bordercolor="#141414",
            font=dict(
                color = "#141414",
                size=12
            )
        ),
        showlegend=True,
        legend=dict(
            title=names,
            orientation="v",
            yanchor="top",
            y=0.9,
            xanchor="left",
            x=1.02,
            font=dict(
                size=12,
                color="#FFFFFF"
            )
        ),
    )
    return fig

create_pie_chart(data, 'Profit Distribution by Organization top ten', 'Organization Name', 'money_value', 10)  # temp call for testing purposes


#----------------------------------------#
# FOLIUM MAP FIGURE
#----------------------------------------#

# this doe snot work at the moment as it needs lat and log 

# look into doing this is addresses 

def create_map(df: pd.DataFrame, lat_col: str, lon_col: str, popup_col: str) -> folium.Map:
    '''
    explanation:
    This function creates a folium map with markers.
    
    input:
    dataframe taken from a sql query that will have the ability to be filtered by various factors.
    
    output:
    folium map object
    
    MANDATORY COLUMNS IN QUERY:
    
    COME BACK TO
    
    additions to make:
    clustering of markers
    different marker styles
    '''
    
    logger.info('Creating folium map with markers')

    # Initialize the map centered around the mean latitude and longitude
    m = folium.Map(location=[df[lat_col].mean(), df[lon_col].mean()], zoom_start=2)

    # Add markers to the map
    for _, row in df.iterrows():
        folium.Marker(
            location=[row[lat_col], row[lon_col]],
            popup=str(row[popup_col])
        ).add_to(m)
    
    return m

# create_map(data, 'Latitude', 'Longitude', 'Organization Name')  # temp call for testing purposes


data.head()






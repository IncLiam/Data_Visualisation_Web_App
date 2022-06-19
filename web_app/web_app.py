"""
Visible at port 8050
"""
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import sqlite3
import pandas as pd


def load_df():
    db_connection = sqlite3.connect("../app_storage/database.db", check_same_thread=False)
    # get past day from database
    dataframe = pd.read_sql_query(f"SELECT * FROM BTCUSDT ORDER BY datetime DESC LIMIT 1440;", db_connection)
    dataframe = dataframe[::-1]
    dataframe["close"] = pd.to_numeric(dataframe["close"])
    dataframe["datetime"] = pd.to_datetime(dataframe.datetime, format="%Y-%m-%d %H:%M:%S")
    dataframe.set_index("datetime", inplace=True)  # set time as index, so we can join them on this shared time
    return dataframe


app = Dash(__name__)

layout = html.Div([

    html.H1(
        children='BTC Trading Bot',
        style={'textAlign': 'center',
               # 'color': colors['text']
               },
    ),

    html.Div(
        children=[html.P('A web app for showing live performance metrics of the bot at every minute for the past '
                         '24 hours.'),
                  html.P(id='button-press-text'),
                  html.Button('Reload data', id='submit-reload', n_clicks=0),
                  html.Br()],
        style={'textAlign': 'center',
               # 'color': colors['text']
               },
    ),

    dcc.Graph(id='graph-with-slider'),

    html.Div(children='look-back (in hours):',
             style={'textAlign': 'left',
                    # 'color': colors['text']
                    },
             ),

    dcc.Slider(
        1,
        24,
        step=None,
        value=1,
        marks={str(i): str(i) for i in range(1, 25)},
        id='hour-slider'
    ),

    dcc.Store(id='store-data', data=load_df().to_json(orient='split'))
])

app.layout = layout


@app.callback(
    Output('graph-with-slider', 'figure'),
    Input('hour-slider', 'value'),
    Input('store-data', 'data'))
def update_figure(selected_hours, data):
    df = pd.read_json(data, orient='split')
    filtered_df = df.iloc[-60 * selected_hours:]
    fig = px.line(filtered_df, x=filtered_df.index, y="close", )
    fig.update_layout(transition_duration=500)
    return fig


@app.callback(
    Output('button-press-text', 'children'),
    Input('store-data', 'data'),
    Input('submit-reload', 'n_clicks'),)
def update_text(data, n_clicks):
    _ = n_clicks  # n_clicks not used
    df = pd.read_json(data, orient='split')
    return f'Prototype showing live close prices from Binance exchange up to {df.iloc[-1].name} (UTC)'


@app.callback(
    Output('store-data', 'data'),
    Input('submit-reload', 'n_clicks'))
def update_data(n_clicks):
    _ = n_clicks  # n_clicks not used
    df = load_df()
    return df.to_json(orient='split')


if __name__ == '__main__':
    app.run_server(debug=False, host="0.0.0.0")

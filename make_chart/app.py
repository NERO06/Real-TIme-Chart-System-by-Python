"""
チャート表示用のプラグラム
PythonのWEBアプリケーションフレームワークDashを利用

レイアウトの内容:
    Interval => 定期発火し、グラフを表示する
    intermediate => data生成と保持
    radioitems => 表示期間の選択
    graph => グラフ表示

データ表示が変更される2つの流れ:
1. intervalを利用した定期更新(初回起動時も含む)
    (1) intervalが発火(intervalごとに発火 -> n_intervalが一つ増加)
    (2) data_format関数発動
    (3) 基本データ更新
    (4) intermediateに新しいデータを渡す
    (5) intermediateからgraphにデータが渡されグラフが表示される

2. radioitemsの変更により表示グラフの対象期間が変更するもの
    (1) radioitemsで対象期間変更
    (2) 対象期間を変更したグラフオブジェクトを生成し、layoutに新しいfigureオブジェクトを返す
    (3) グラフの表示が変更される
"""

import configparser
import datetime
import json
import os
import time

import dash
from dash.dependencies import Input
from dash.dependencies import Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pytz

from db import fetch_db

cfg = configparser.ConfigParser()
config_file = os.path.join(os.path.abspath("."), "config.ini")
cfg.read(config_file)

db_config = {
    "user": cfg["DBserverA"]["USER"],
    "host": cfg["DBserverA"]["HOST"],
    "password": cfg["DBserverA"]["PASSWORD"]
}


def to_datetime(ts):
    """
    テーブルから取り出したタイムスタンプを時刻オブジェクトに帰る
    :param ts: <str> タイムスタンプ
    :return: Datetimeオブジェクト
    """
    tokyo = pytz.timezone('Asia/Tokyo')
    dt_obj = datetime.datetime.fromtimestamp(ts, tokyo)
    return dt_obj


def xaxis(d_period, dt):
    """
    x軸のレイアウトを返す
    :param d_period: 表示するチャートの期間
    :param dt: x軸の対象期間
    :return: パターン1[1か月チャート、1週間チャート]、パターン2[1日チャート、6hチャート,1hチャート]
    """
    pattern = [{'type': 'date', 'range': dt, 'fixedrange': True, 'showline': True, 'linecolor': 'dimgrey',
                'tickfont': {'size': 12}},
               {'type': 'date', 'range': dt, 'fixedrange': True, 'showline': True, 'linecolor': 'dimgrey',
                'tickfont': {'size': 12}}]
    if (d_period == "data1m") or (d_period == "data1w"):
        return pattern[0]
    else:
        return pattern[1]


def yaxis(d_period):
    """
    yy軸のレイアウトを返す
    :param d_period: 表示するチャートの期間
    :return: パターン1[1ヶ月チャート、1週間チャート、1日チャート]、パターン2[6hチャート、1hチャート]
    """
    pattern = [{'fixedrange': True, 'showline': True, 'linecolor': 'dimgrey',
                "tickmode": "auto", "nticks": 7},
               {'fixedrange': True, 'showline': True, 'linecolor': 'dimgrey',
                "tickmode": "auto", "nticks": 7, "tickformat": ",d"}]
    if (d_period == "data1m") or (d_period == "data1w") or (d_period == "data1d"):
        return pattern[0]
    else:
        return pattern[1]


app = dash.Dash(__name__)

app.layout = html.Div(children=[
    html.P(id="bbs", children="Presents by Blow Up By Black Swan."),
    html.H1("Real Time Chart System by Plotly Dash, Python"),
    html.Div([html.Div([html.Div(id="current_price"),
                        html.Div([html.Span("latest price "),
                                  html.Span(id="dealtime")],
                                 style={'height': '50%',
                                        'textAlign': 'center'})],
                       style={'float': 'left', 'width': '40%'}
                       ),
              html.Div([dcc.RadioItems(id="radioitem",
                                       options=[
                                           {"label": "1 month", "value": "data1m"},
                                           {"label": "1 week", "value": "data1w"},
                                           {"label": "1 day", "value": "data1d"},
                                           {"label": "6 hours", "value": "data6h"},
                                           {"label": "1 hour", "value": "data1h"}
                                       ],
                                       value="data1m",
                                       labelStyle={"display": "inline-block"}),
                        dcc.Graph(id="graph",
                                  config={'displayModeBar': False}),
                        ],
                       style={'float': 'right', 'width': '80%'})
              ],
             style={'display': 'flex'}
             ),
    html.Div(id="intermediate", style={"display": "none"}),
    dcc.Interval(id="interval",
                 interval=10 * 1000,
                 n_intervals=0),
])


@app.callback(Output("intermediate", "children"),
              [Input("interval", "n_intervals")])
def data_format(n):
    """
    テーブルからデータを取得し、整形する
    (1)dcc.Intervalが発火(アクセス時も含む)
    (2)関数が作動:
        1. 現在時刻計算
        2. 対象テーブルから現在時刻から換算した対象期間のデータを全てのチャート表示期間用に取得
        3. 各データを格納したリストをJson化し、intermediateに返す
           ※ コールバック間でデータを渡すためにはJson化が必須
    :param n: <int> n_intervalのこと。トリガーの役割以外利用せず
    :return data_dict: <dict> 各チャート用のデータが入ったdict
    """

    # 1. 現在時刻計算
    current_time = time.time()

    # 2. 対象テーブルから現在時刻から換算した対象期間のデータを全てのチャート表示期間用に取得
    data1m = fetch_db(db_config, current_time, "data1m")
    data1w = fetch_db(db_config, current_time, "data1w")
    data1d = fetch_db(db_config, current_time, "data1d")
    data6h = fetch_db(db_config, current_time, "data6h")
    data1h = fetch_db(db_config, current_time, "data1h")
    data_dict = {"data1m": data1m, "data1w": data1w, "data1d": data1d, "data6h": data6h, "data1h": data1h}
    return json.dumps(data_dict)


@app.callback(Output("graph", "figure"),
              [Input("radioitem", "value"),
               Input("intermediate", "children")])
def make_figure(value, json_data):
    """
    グラフ表示用のためのfigureオブジェクトを返す
    (1)トリガー発動: radiotimesのvalueが変更 or intermediateのchildrenが変更
    (2)関数が作動:
        1. intermediateのchildrenにあるデータをJsonからpythonのデータに変換
        2. radioitemsのvalueに一致するデータ取り出し
        3. グラフ用にデータを整形
        4. figureオブジェクトを返す
    :param value: <str> radioitemsのの値(チャートの表示期間を表す)
    :param json_data: intermediateのchildrenにあるJsonデータ
    :return figure: <figure> グラフ表示のためのfigureオブジェクト
    """
    # 1. intermediateのchildrenにあるデータをJsonからpythonのデータに変換
    data_dict = json.loads(json_data)
    # 2. radioitemsのvalueに一致するデータ取り出し
    for i in data_dict:
        if i == value:
            data = data_dict[i]
        continue
    # 3. グラフ用にデータを整形
    dealtimes = [to_datetime(i[0]) for i in data]
    dt = [dealtimes[0], dealtimes[-1]]
    price = ["{:,}".format(int(i[1])) for i in data]
    trace = go.Scatter(x=dealtimes,
                       y=price,
                       mode="lines",
                       line={'color': 'lightcoral',
                             'width': 2},
                       hoverinfo="x+y")
    ex_layout = {'dragmode': 'turntable',
                 'hoverlabel': {'bgcolor': 'linen',
                                'bordercolor': 'pink',
                                'font': {'size': 12,
                                         'color': 'dimgrey'}},
                 'xaxis': xaxis(value, dt),
                 'yaxis': yaxis(value)
                 }
    figure = go.Figure(data=[trace], layout=ex_layout)
    return figure


@app.callback(Output("current_price", "children"),
              [Input("intermediate", "children")])
def show_current_price(json_data):
    """
    現在価格を表すために利用するメソッド
    :param json_data: intermediateのchildrenに格納されているJsonでーた
    :return: <str> 現在価格
    """
    data_dict = json.loads(json_data)
    price = data_dict["data1h"][-1][1]
    current_price = "  " + "{:,}".format(int(price)) + " YEN  "
    return current_price


@app.callback(Output("dealtime", "children"),
              [Input("intermediate", "children")])
def show_current_time(json_data):
    """
    現在時刻を表示するためのメソッド
    :param json_data: intermediateのchildrenに格納されているJsonでーた
    :return: <str> 現在時刻
    """
    data_dict = json.loads(json_data)
    dt = to_datetime(data_dict["data1h"][-1][0])
    dealtime = "(" + dt.strftime('%H:%M:%S') + ")"
    return dealtime


# 実行プログラム
if __name__ == "__main__":
    app.run_server(debug=False)

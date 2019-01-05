"""
DBの取り扱いに関する関数を格納
get_db: DBとの接続
fetch_db: DBからデータを取得
"""
import mysql.connector


def get_db(config):
    """
    DBserverとの接続用
    :param config: 設定情報
    :return: Connectionオブジェクト、 Cursorオブジェクト
    """
    try:
        con_obj = mysql.connector.connect(**config)
    except Exception as err:
        exit()
    cur_obj = con_obj.cursor()
    return con_obj, cur_obj


def fetch_db(config, current_time, d_period):
    """
    DBからデータを取得し、返す関数
    (1)基本情報取得(DB設定, 発動時点, 対象期間)
    (2)SQL文の組成・実行
    (3)データを納めた変数をリスト化し返す
    :param config: DBの設定
    :param current_time: <int> 現在時刻
    :param d_period: <str>グラフ表示期間。data period
    :return: <list> データが入ったリスト
    """
    ts1m = 60 * 60 * 24 * 30 + 60 * 60 * 24
    ts1w = 60 * 60 * 24 * 7 + 60 * 60 * 6
    ts1d = 60 * 60 * 24 + 60 * 60
    ts6h = 60 * 60 * 6 + 60 * 10
    ts1h = 60 * 60 + 60 * 5
    tss = {"data1m": ts1m,
           "data1w": ts1w,
           "data1d": ts1d,
           "data6h": ts6h,
           "data1h": ts1h}
    tables = {"data1m": "bf_amonth",
              "data1w": "bf_aweek",
              "data1d": "bf_aday",
              "data6h": "bf_base",
              "data1h": "bf_base"}
    ts = [tss[i] for i in tss if i == d_period]
    table = [tables[i] for i in tables if i == d_period]
    query_base = "SELECT timestamp, price FROM blowupbbs_crypto.{} WHERE {} < timestamp;"
    query = query_base.format(table[0], (current_time - ts[0]))
    con_obj, cur_obj = get_db(config)
    cur_obj.execute(query)
    data = cur_obj.fetchall()
    query_last = "SELECT timestamp, price FROM blowupbbs_crypto.bf_base ORDER BY no DESC LIMIT 1;"
    cur_obj.execute(query_last)
    last_data = cur_obj.fetchone()
    data.append(last_data)
    con_obj.close()
    cur_obj.close()
    return data

#! シバン行

"""
ベーステーブルから1ヶ月,1週間,1日表記用のテーブルにデータを振り分けるための実行ファイル。
DB接続設定: DBserverA
amonth(): 1ヶ月表記用テーブルへのデータの保存
aweek() : ワンウィーク表記用テーブルへのデータの保存
aday()  : ワンデイ表記用テーブルへのデータの保存
"""
import configparser
import os
import time
from db import get_db

config_file = os.path.join(os.path.abspath('.'), "config.ini")
cfg = configparser.ConfigParser()
cfg.read(config_file)

config = {
    "user": cfg["DBserverT"]["USER"],
    "host": cfg["DBserverT"]["HOST"],
    "password": cfg["DBserverT"]["PASSWORD"]
}

start_ts = float(cfg['SORT_CONFIG']['START_TIME'])

confirm_query = "SELECT timestamp from blowupbbs_crypto.bf_base ORDER BY no DESC LIMIT 1;"
select_query = "SELECT * FROM blowupbbs_crypto.bf_base WHERE timestamp >= %s AND %s > timestamp limit 1;"

sec2h = 60 * 60 * 2
sec30m = 60 * 30
sec5m = 60 * 5


# データ取得関連
def amonth(cur_obj, query, start_ts):
    ts = (start_ts, start_ts + (60 * 60 * 2))
    cur_obj.execute(query, ts)
    data = cur_obj.fetchone()
    if data is None:
        return False
    insert_query = "INSERT INTO blowupbbs_crypto.bf_amonth (base_no, id, price, timestamp) VALUE (%s,%s,%s,%s);"
    try:
        cur_obj.execute(insert_query, data)
        con_obj.commit()
    except Exception as err:
        cur_obj.close()
        con_obj.close()
        print(err)
        exit()
    return True


def aweek(cur_obj, query, start_ts):
    for i in range(4):
        ts = (start_ts, start_ts + (60 * 30))
        cur_obj.execute(query, ts)
        data = cur_obj.fetchone()
        if data is None:
            start_ts += (60 * 30)
            continue
        insert_query = "INSERT INTOblowupbbs_crypto.bf_aweek (base_no, id, price, timestamp) VALUE (%s,%s,%s,%s);"
        try:
            cur_obj.execute(insert_query, data)
            con_obj.commit()
        except Exception as err:
            cur_obj.close()
            con_obj.close()
            print(err)
            exit()
        start_ts += (60 * 30)
    return True


def aday(cur_obj, query, start_ts):
    for i in range(24):
        ts = (start_ts, start_ts + (60 * 5))
        cur_obj.execute(query, ts)
        data = cur_obj.fetchone()
        if data is None:
            start_ts += (60 * 5)
            continue
        insert_query = "INSERT INTO blowupbbs_crypto.bf_aday (base_no, id, price, timestamp) VALUE (%s,%s,%s,%s);"
        try:
            cur_obj.execute(insert_query, data)
            con_obj.commit()
        except Exception as err:
            cur_obj.close()
            con_obj.close()
            print(err)
            exit()
        start_ts += (60 * 5)
    return True


# プログラムスタート
if __name__ == "__main__":
    con_obj, cur_obj = get_db(config)
    while True:
        # for i in range(5):    #確認用
        current_time = time.time()
        if (start_ts + sec2h) <= current_time:
            v = amonth(cur_obj, select_query, start_ts)
            if not v:
                start_ts += sec2h
                continue
            aweek(cur_obj, select_query, start_ts)
            aday(cur_obj, select_query, start_ts)
        elif (start_ts + sec30m) <= current_time < (start_ts + sec2h):
            v = aweek(cur_obj, select_query, start_ts)
            if not v:
                break
            aday(cur_obj, select_query, start_ts)
            break
        elif (start_ts + sec5m) <= current_time < (start_ts + sec30m):
            aday(cur_obj, select_query, start_ts)
            break
        else:
            break
        start_ts += sec2h
    cur_obj.close()
    con_obj.close()

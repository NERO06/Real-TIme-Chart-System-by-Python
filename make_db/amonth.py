"""
ワンマンス表記用のテーブルを作成するための実行ファイル
過去データはsort_to_data.pyで仕分け、リアルタイムはこのamonth.pyで仕分け
※ cronで5分ごとに実行する
(1) 設定の読み込み
(2) 現在時刻とbf_amonthテーブルの最終データ時刻を比較
    5分以上 -> sort_to_table.pyが稼働中であり、このプログラムの実行機会でないため、実行終了
    5分未満 -> bf_baseテーブルからデータを1つ取得し、bf_amonthテーブルに追加
"""

"""
1ヶ月表記用のテーブルを作成するための実行ファイル。
cronで2時間分ごとに実行する
DB接続設定: DBserverE
"""
import configparser
import os
import time
from db import get_db

# (1) 設定の読み込み
config_file = os.path.join(os.path.abspath('.'), "config.ini")
cfg = configparser.ConfigParser()
cfg.read(config_file)

config = {
    "user": cfg["DBserverT"]["USER"],
    "host": cfg["DBserverT"]["HOST"],
    "password": cfg["DBserverT"]["PASSWORD"]
}

confirm_query = "SELECT timestamp FROM blowupbbs_crypto.bf_amonth ORDER BY no DESC LIMIT 1;"
select_query = "SELECT * FROM blowupbbs_crypto.bf_base  ORDER BY no DESC limit 1;"
insert_query = "INSERT INTO blowupbbs_crypto.bf_amonth (base_no, id, price, timestamp) VALUE (%s,%s,%s,%s);"
current_time = time.time()

con_obj, cur_obj = get_db(config)
cur_obj.execute(confirm_query)
last_time = cur_obj.fetchone()[0]
# (2) 現在時刻とbf_amonthテーブルの最終データ時刻を比較
if (current_time - last_time) > (60*60*2):
    pass
else:
    current_time = (current_time,)
    cur_obj.execute(select_query)
    data = cur_obj.fetchone()
    cur_obj.execute(insert_query, data)
    con_obj.commit()
cur_obj.close()
con_obj.close()

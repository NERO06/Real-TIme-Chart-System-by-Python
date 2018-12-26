#! シバン行

"""
ワンデイ表記用のテーブルを作成するための実行ファイル。
cronで5分ごとに実行する
DB接続設定: DBserverC
"""
import configparser
import os
import time
from db import get_db

config_file = os.path.join(os.path.abspath('.'), "config.ini")
cfg = configparser.ConfigParser()
cfg.read(config_file)

config = {
    "user": cfg["DBserverA"]["USER"],
    "host": cfg["DBserverA"]["HOST"],
    "password": cfg["DBserverA"]["PASSWORD"]
}

confirm_query = "SELECT timestamp FROM blowupbbs_crypto.bf_aday ORDER BY no DESC LIMIT 1;"
select_query = "SELECT * FROM blowupbbs_crypto.bf_base ORDER BY no DESC limit 1;"
insert_query = "INSERT INTO blowupbbs_crypto.bf_aday (base_no, id, price, timestamp) VALUE (%s,%s,%s,%s);"
current_time = time.time()

con_obj, cur_obj = get_db(config)
cur_obj.execute(confirm_query)
last_time = cur_obj.fetchone()[0]
if (current_time - last_time) > (60*5):
    pass
else:
    current_time = (current_time,)
    cur_obj.execute(select_query)
    data = cur_obj.fetchone()
    cur_obj.execute(insert_query, data)
    con_obj.commit()
cur_obj.close()
con_obj.close()

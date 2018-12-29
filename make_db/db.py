#! シバン行
"""
DBの取り扱いに関する関数を格納
get_db(): DBとの接続
          params -> nothing
          config -> from config.ini
make_db(): DBと関連テーブルの作成(基本は初回の1回しか実行しない)
insert_db(): DBへのデータ保存
"""
import mysql.connector


# DBserverとの接続用メソッド
def get_db(config):
    try:
        con_obj = mysql.connector.connect(**config)
    except Exception as err:
        exit()
    cur_obj = con_obj.cursor()
    return con_obj, cur_obj


# DBの作成
def make_db(queries, config):
    con_obj, cur_obj = get_db(config)
    for query in queries:
        cur_obj.execute(query)
    cur_obj.close()
    con_obj.close()


# データ保存用
def insert_db(con_obj, cur_obj, data_list):
    insert_query = (
        "INSERT INTO blowupbbs_crypto.bf_base (id, price, timestamp) "
        "VALUES (%s, %s, %s);"
    )
    cur_obj.executemany(insert_query, data_list)
    con_obj.commit()

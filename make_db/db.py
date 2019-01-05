#! シバン行
"""
DBの取り扱いに関する関数を格納
get_db(): DBとの接続
make_db(): DBと関連テーブルの作成(基本は初回の1回しか実行しない)
insert_db(): DBへのデータ保存
"""
import mysql.connector


def get_db(config):
    """
    DB serverとの接続用メソッド
    :param config: DBと接続するための設定ファイル
    :return: Connectionオブジェクト, Cursorオブジェクト
    """
    try:
        con_obj = mysql.connector.connect(**config)
    except Exception as err:    # エラーが出た場合の処理
        exit()
    cur_obj = con_obj.cursor()
    return con_obj, cur_obj


def make_db(queries, config):
    """
    DBを作成するメソッド
    :param queries: SQL文
    :param config: DBと接続するための設定ファイル
    :return: nothing
    """
    con_obj, cur_obj = get_db(config)
    for query in queries:
        cur_obj.execute(query)
    cur_obj.close()
    con_obj.close()


# データ保存用
def insert_db(con_obj, cur_obj, data_list):
    """
    DBにデータを追加するメソッド
    :param con_obj: Connectoinオブジェクト
    :param cur_obj: Cursorオブジェクト
    :param data_list: <list> 追加データのリスト
    :return: nothing
    """
    insert_query = (
        "INSERT INTO blowupbbs_crypto.bf_base (id, price, timestamp) "
        "VALUES (%s, %s, %s);"
    )
    cur_obj.executemany(insert_query, data_list)
    con_obj.commit()

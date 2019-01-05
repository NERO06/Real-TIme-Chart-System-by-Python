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
    "user": cfg["DBserverB"]["USER"],
    "host": cfg["DBserverB"]["HOST"],
    "password": cfg["DBserverB"]["PASSWORD"]
}

start_ts = float(cfg['SORT_CONFIG']['START_TIME'])

confirm_query = "SELECT timestamp from blowupbbs_crypto.bf_base ORDER BY no DESC LIMIT 1;"
select_query = "SELECT * FROM blowupbbs_crypto.bf_base WHERE timestamp >= %s AND %s > timestamp limit 1;"

sec2h = 60 * 60 * 2
sec30m = 60 * 30
sec5m = 60 * 5


def amonth(cur_obj, query, start_ts):
    """
    1ヶ月表記用テーブルの作成・更新
    2時間で1つのデータをbf_amonthテーブルに追加する
    (1) bf_baseテーブルからデータ取得
      想定事例: 取得するデータがなく空のリストが返ってきた場合 -> Falseを返す
    (2) bf_amonthテーブルにデータを追加
    :param cur_obj: Cursorオブジェクト
    :param query:  SQL文
    :param start_ts: <int> スタートタイム
    :return: データの追加完了した場合 -> True, データの追加がなかった場合 -> False
    """
    time.sleep(0.6)
    ts = (start_ts, start_ts + (60 * 60 * 2))
    # (1)SQL文の実行
    cur_obj.execute(query, ts)
    data = cur_obj.fetchone()
    # 想定事例: 取得するデータがなく空のリストが返ってきた場合 -> Falseを返す
    if data is None:
        return False
    insert_query = "INSERT INTO blowupbbs_crypto.bf_amonth (base_no, id, price, timestamp) VALUE (%s,%s,%s,%s);"
    # (2)bf_amonthテーブルにデータを追加
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
    """
    1週間表記用テーブルの作成・更新
    2時間分のデータを30分に1つ計4つを取得する
    ・ループ
    (1) bf_baseテーブルからデータ取得
      想定事例: 取得するデータがなく空のリストが返ってきた場合 -> Falseを返す
    (2) bf_aweekテーブルにデータを追加
    :param cur_obj:  Cursorオブジェクト
    :param query:  SQL文
    :param start_ts: <int> スタートタイム
    :return: True
    """
    for i in range(4):
        time.sleep(0.6)
        ts = (start_ts, start_ts + (60 * 30))
        # (1) bf_baseテーブルからデータ取得
        cur_obj.execute(query, ts)
        data = cur_obj.fetchone()
        # 想定事例: 取得するデータが空のリストで返ってきた場合 -> 次の区間へ進む
        if data is None:
            start_ts += (60 * 30)
            continue
        insert_query = "INSERT INTO blowupbbs_crypto.bf_aweek (base_no, id, price, timestamp) VALUE (%s,%s,%s,%s);"
        # (2) bf_aweekテーブルにデータを追加
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
    """
    1日表記用テーブルの作成・更新
    2時間分のデータを5分に1つ計24つを取得する
    ・ループ
    (1) bf_baseテーブルからデータ取得
      想定事例: 取得するデータがなく空のリストが返ってきた場合 -> Falseを返す
    (2) bf_adayテーブルにデータを追加
    :param cur_obj: Cursorオブジェクト
    :param query: SQL文
    :param start_ts: <int> スタートタイム
    :return: Ture
    """
    for i in range(24):
        time.sleep(0.6)
        ts = (start_ts, start_ts + (60 * 5))
        # (1) bf_baseテーブルからデータ取得
        cur_obj.execute(query, ts)
        data = cur_obj.fetchone()
        # 想定事例: 取得するデータがなく空のリストが返ってきた場合 -> Falseを返す
        if data is None:
            start_ts += (60 * 5)
            continue
        insert_query = "INSERT INTO blowupbbs_crypto.bf_aday (base_no, id, price, timestamp) VALUE (%s,%s,%s,%s);"
        # (2) bf_adayテーブルにデータを追加
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


# 実行プログラム
"""
ループを回して各期間用テーブルを作成
スタートタイム = 整理を開始するタイムスタンプ
(1)スタートタイムと現在時刻に2時間以上の隔たりがある
  この2時間内でデータが存在しない -> 次の2時間に進む
                    存在する -> amonthとaweekメソッド実行
(2)スタートタイム+30分 < 現在時刻 < スタートタイム+2時間
  対象期間にデータがない -> 振り分けプログラム終了
                 ある -> adayメソッドを実行し振り分けプログラム終了
(3)スタートタイム+5分 < 現在時刻 < スタートタイム+30分
  adayメソッドを実行し振り分けプログラム終了
(4)現在時刻 < スタートタイム+5分
  振り分けプログラム終了
 
"""
if __name__ == "__main__":
    con_obj, cur_obj = get_db(config)
    while True:
        time.sleep(0.6)
        current_time = time.time()
        # (1) 各期間用テーブルへのデータ振り分けが不十分な場合
        if (start_ts + sec2h) <= current_time:
            v = amonth(cur_obj, select_query, start_ts)
            if not v:
                start_ts += sec2h
                continue
            aweek(cur_obj, select_query, start_ts)
            aday(cur_obj, select_query, start_ts)
        # (2)スタートタイム+30分 < 現在時刻 < スタートタイム+2時間
        elif (start_ts + sec30m) <= current_time < (start_ts + sec2h):
            v = aweek(cur_obj, select_query, start_ts)
            if not v:
                break
            aday(cur_obj, select_query, start_ts)
            break
        # (3)スタートタイム+5分 < 現在時刻 < スタートタイム+30分
        elif (start_ts + sec5m) <= current_time < (start_ts + sec30m):
            aday(cur_obj, select_query, start_ts)
            break
        # (4)現在時刻 < スタートタイム+5分
        else:
            break
        start_ts += sec2h
    cur_obj.close()
    con_obj.close()

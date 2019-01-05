"""
ベースとなるテーブルを作るための実行ファイル
BF_apiクラス
  __init__ -> コンストラクタ
  to_timestamp -> 取得したAPIデータ内の時刻データをタイムスタンプに変換
  slack_error -> slackへの連絡用メソッド
  past_data -> 過去データを取得するメソッド
  real_data -> リアルタイムでAPIからデータを取得するメソッド
"""

import configparser
import datetime
import json
import os
import time
import traceback

import pytz
import requests

from db import get_db, make_db, insert_db
# from schema import db_schema


class BF_api:
    def __init__(self):
        """
        コンストラクタ
        """
        config_path = os.path.join(os.path.abspath("."), "config.ini")
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        self.api_url = cfg["API_CONFIG"]["URL"]
        self.count = cfg["API_CONFIG"]["COUNT"]
        self.after = cfg["API_CONFIG"]["AFTER"]
        self.before = cfg["API_CONFIG"]["BEFORE"]
        self.config = {
            "user": cfg["DBserverT"]["USER"],
            "host": cfg["DBserverT"]["HOST"],
            "password": cfg["DBserverT"]["PASSWORD"]
        }
        self.slack_url = cfg["SLACK_CONFIG"]["SLACK_URL"]

    def to_timestamp(self, dt):
        """
        APIから取得したデータ内の時刻データをタイムスタンプに変換
        :param dt: <str> 時刻データ
        :return: <int> タイムスタンプ
        """
        try:
            dt_obj = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f")
        except:
            dt_obj = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
        tz_obj = pytz.utc.localize(dt_obj)
        ts = tz_obj.timestamp()
        return ts

    def slack_error(self, concept, err, err_c, place):
        """
        エラー情報をslackに送るメソッド
        :param concept: エラー概要
        :param err: エラー内容1
        :param err_c: エラー内容2
        :param place: エラー個所
        :return: nothing
        """
        slack_url = self.slack_url
        payload = {
            "attachments": [
                {
                    "pretext": "*ERROR*",
                    "title": "{}".format(concept),
                    "text": "error: {0}\nerror contents: {1}\nplace: {2}".format(err, err_c, str(place)),
                    "mrkdwn_in": ["pretext"]
                }
            ]
        }
        requests.post(slack_url, data=json.dumps(payload))

    # param設定-> ループ【apiアクセス -> json化 -> データの並び替え -> データ保存 -> param修正】 -> con,curクローズ
    def past_data(self):
        """
        APIから過去データを取得するメソッド
        (1) param設定
        (2)ループ
            (2-1)APIアクセス
                (想定事例1) APIに問題がある(ステータスコードが200でない) -> slackへ連絡
                (想定事例2) idで決めた区間にデータがない場合(空のリスト)
                    2-1 現在時刻近辺までデータ取得が進んでいる -> ループ抜け出しreal_dataの呼び出しへ
                    2-2 まだ十分に過去データが取得できていない -> 次のid区間のAPIデータ取得へ
            (2-2)データの並び替え・整理
            (2-3)データ保存
            (2-4)param修正
        (3)DBとのコネクション切断
        ※ ところどころにある"time.sleep(1)はAPIの呼び出し制限に引っかからない様にするため
        :return: <int> last_id -> real_dateメソッドにAPIデータの重複や空白なく引き継ぐために利用
        """
        # (1)param設定
        params = {'count': self.count,
                  'after': self.after,
                  'before': self.before
                  }

        con_obj, cur_obj = get_db(self.config)
        last_ts = 0

        # (2)ループ
        while True:
            # (2-1)APIアクセス
            api_data_json = requests.get(self.api_url, params)
            # 想定事例1処理: APIに問題がある -> slackに連絡
            if api_data_json.status_code != 200:
                concept = "api server error"
                err = "status code error"
                try:
                    err_c = api_data_json.json()
                except Exception as err:
                    er = "json error"
                    err_c = err
                place = params['after']
                self.slack_error(concept, er, err_c, place)
                break
            api_data = api_data_json.json()
            # 想定事例2処理: 対象区間にデータが存在しない ->
            if not api_data:
                current_time = time.time()
                # 想定エラー2-1 現在時刻近辺までデータ取得が進んでいる -> ループ抜け出しreal_dataの呼び出しへ
                if (current_time - last_ts) <= (60 * 5):
                    break
                # 想定エラー2-2 まだ十分に過去データが取得できていない -> 次のid区間のAPIデータ取得へ
                params['before'] = int(params['before']) + 500
                time.sleep(0.1)
                continue
            # (2-2)データの並び替え・整理
            api_data.sort(key=lambda x: x['id'])
            data_list = []
            for i in range(len(api_data)):
                data_list.append((api_data[i]['id'],
                                  api_data[i]['price'],
                                  self.to_timestamp(api_data[i]['exec_date'])))  # APIデータの整形作業完成
            last_id = api_data[-1]['id']
            last_ts = data_list[-1][-1]

            # (2-3)データ保存部分
            try:
                insert_db(con_obj, cur_obj, data_list)
            except Exception as err_c:    # エラーが生じた場合の対応
                concept = "DB insert error"
                err = "Mistake executing sql query"
                place = api_data[0]['id']
                self.slack_error(concept, err, err_c, place)
                break
            # (2-4)paramの再設定
            params['after'] = last_id
            params['before'] = last_id + 500
            time.sleep(0.1)

        # (3)DBとのコネクション切断
        cur_obj.close()
        con_obj.close()
        print("end: past_data()")
        return last_id    # real_dateでつつがなく引き継ぐため

    def real_data(self, last_id):
        """
        リアルタイムデータ取得メソッド
        (1) param設定
        (2)ループ
            (2-1)APIアクセス
                (想定エラー1) APIに問題がある(ステータスコードが200でない) -> slackへ連絡
                (想定エラー2) idで決めた区間にデータがない場合(空のリスト) -> 時間を置いて再度(1)APIアクセス
            (2-2)データの並び替え・整理
            (2-3)データ保存
            (2-4)param修正
        :param last_id: <int> past_dateメソッドから返されたもの
        :return: nothing
        """
        # (1)param設定
        params = {'count': self.count,
                  'after': last_id}

        con_obj, cur_obj = get_db(self.config)

        # (2)ループ
        while True:
            # (2-1)APIアクセス
            api_data_json = requests.get(self.api_url, params)
            # (想定事例1) APIに問題がある(ステータスコードが200でない) -> slackへ連絡
            if api_data_json.status_code != 200:
                concept = "api server error"
                err = "status code error"
                err_c = traceback.format_exc()
                place = params['after']
                self.slack_error(concept, err, err_c, place)
                exit()
            api_data = api_data_json.json()
            # (想定事例2) idで決めた区間にデータがない場合(空のリスト) -> 時間を置いて再度(1)APIアクセス
            if not api_data:
                time.sleep(3)
                continue
            # (2-2)データの並び替え・整理
            api_data.sort(key=lambda x: x['id'])
            data_list = []
            for i in range(len(api_data)):
                data_list.append([api_data[i]['id'],
                                  api_data[i]['price'],
                                  self.to_timestamp(api_data[i]['exec_date'])])
            print(api_data[-1]['exec_date'])
            last_id = api_data[-1]['id']

            # (3)データ保存
            try:
                insert_db(con_obj, cur_obj, data_list)
            except Exception as err_c:
                concept = "DB insert error"
                err = "Mistake executing sql query"
                place = api_data[0]['id']
                self.slack_error(concept, err, err_c, place)
                break
            # (4)param修正
            params['after'] = last_id
            time.sleep(3)


# 実行プログラム
if __name__ == '__main__':
    bf = BF_api()
    make_db(db_schema, bf.config)
    last_id = bf.past_data()
    bf.real_data(last_id)

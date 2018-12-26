#! シバン行

"""
ベースとなるテーブルを作るための実行ファイル
DB接続設定: DBserverA
本番のparam:AFTER = 588931154
           BEFORE = 588931654

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
from schema import db_schema


class BF_api:
    def __init__(self):
        self.config_path = os.path.join(os.path.abspath("."), "config.ini")
        cfg = configparser.ConfigParser()
        cfg.read(self.config_path)
        self.api_url = cfg["API_CONFIG"]["URL"]
        self.count = cfg["API_CONFIG"]["COUNT"]
        self.after = cfg["API_CONFIG"]["AFTER"]
        self.before = cfg["API_CONFIG"]["BEFORE"]
        self.config = {
            "user": cfg["DBserverA"]["USER"],
            "host": cfg["DBserverA"]["HOST"],
            "password": cfg["DBserverA"]["PASSWORD"]
        }
        self.slack_url = cfg["SLACK_CONFIG"]["SLACK_URL"]

    # convert time object to UNIX timestamp
    def to_timestamp(self, dt):
        try:
            dt_obj = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f")
        except:
            dt_obj = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
        tz_obj = pytz.utc.localize(dt_obj)
        ts = tz_obj.timestamp()
        return ts

    # slack連絡用
    def slack_error(self, concept, err, err_c, place):
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

    # 過去データ取得メソッド
    # param設定-> ループ【apiアクセス -> json化 -> データの並び替え -> データ保存 -> param修正】 -> con,curクローズ
    def past_data(self):
        params = {'count': self.count,
                  'after': self.after,
                  'before': self.before
                  }

        con_obj, cur_obj = get_db(self.config)
        last_ts = 0

        while True:
            # for num in range(2):

            # APIデータ取得から整形まで
            api_data_json = requests.get(self.api_url, params)
            if api_data_json.status_code != 200:
                # slackに連絡
                concept = "api server error"
                err = "status code error"
                err_c = api_data_json.json()
                place = params['after']
                self.slack_error(concept, err, err_c, place)
                break

            api_data = api_data_json.json()
            if not api_data:
                current_time = time.time()
                if (current_time - last_ts) <= (60 * 5):
                    break
                params['before'] += 500
                time.sleep(1)
                continue
            api_data.sort(key=lambda x: x['id'])
            data_list = []
            for i in range(len(api_data)):
                data_list.append((api_data[i]['id'],
                                  api_data[i]['price'],
                                  self.to_timestamp(api_data[i]['exec_date'])))  # APIデータの整形作業完成
            print(api_data[-1]['exec_date'])
            last_id = api_data[-1]['id']
            last_ts = data_list[-1][-1]

            # データ保存部分
            try:
                insert_db(con_obj, cur_obj, data_list)
            except Exception as err_c:
                concept = "DB insert error"
                err = "Mistake executing sql query"
                place = api_data[0]['id']
                self.slack_error(concept, err, err_c, place)
                break
            params['after'] = last_id
            params['before'] = last_id + 500
            time.sleep(0.09)

        # DBとのコネクション切断
        cur_obj.close()
        con_obj.close()
        print("end: past_data()")
        return last_id

    # リアルタイムデータ取得メソッド
    def real_data(self, last_id):
        params = {'count': self.count,
                  'after': last_id}

        # connectionオブジェクト生成
        con_obj, cur_obj = get_db(self.config)

        while True:
            # for num in range(2):
            api_data_json = requests.get(self.api_url, params)
            if api_data_json.status_code != 200:
                concept = "api server error"
                err = "status code error"
                err_c = traceback.format_exc()
                place = params['after']
                self.slack_error(concept, err, err_c, place)
                exit()
            api_data = api_data_json.json()
            if not api_data:
                time.sleep(30)
                params['after'] = last_id
                continue
            api_data.sort(key=lambda x: x['id'])
            data_list = []
            for i in range(len(api_data)):
                data_list.append([api_data[i]['id'],
                                  api_data[i]['price'],
                                  self.to_timestamp(api_data[i]['exec_date'])])
            print(api_data[-1]['exec_date'])
            last_id = api_data[-1]['id']

            # データ保存部分
            try:
                insert_db(con_obj, cur_obj, data_list)
            except Exception as err_c:
                concept = "DB insert error"
                err = "Mistake executing sql query"
                place = api_data[0]['id']
                self.slack_error(concept, err, err_c, place)
                break
            params['after'] = last_id
            time.sleep(15)


if __name__ == '__main__':
    bf = BF_api()
    make_db(db_schema, bf.config)
    last_id = bf.past_data()
    bf.real_data(last_id)

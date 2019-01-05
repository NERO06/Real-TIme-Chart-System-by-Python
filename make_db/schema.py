"""
スキーマの構造
1. 同名データベースがあれば削除
2. データベース作成(blowupbbs_crypto)
3. ベースとなるテーブル作成(bf_base)
   column: no, id, price, timestamp
4. 各憑依期間に対応するテーブルを作成(bf_aday, bf_aweek, bf_amonth)
   column: no, base_no(bf_baseのno), id, price, timestamp

"""

db_schema = [
    "DROP DATABASE IF EXISTS blowupbbs_crypto;",  # DB作成
    "CREATE DATABASE blowupbbs_crypto;",
    "CREATE TABLE blowupbbs_crypto.bf_base (no int AUTO_INCREMENT UNIQUE NOT NULL,"
                             "id int NOT NULL,"
                             "price DOUBLE(9,1) NOT NULL,"
                             "timestamp DOUBLE(13,3) NOT NULL,"
                             "primary key(id), index(no));",
    "CREATE TABLE blowupbbs_crypto.bf_aday (no int AUTO_INCREMENT UNIQUE NOT NULL,"
                             "base_no int NOT NULL,"
                             "id int NOT NULL,"
                             "price DOUBLE(9,1) NOT NULL,"
                             "timestamp DOUBLE(13,3) NOT NULL,"
                             "primary key(id),index(no));",
    "CREATE TABLE blowupbbs_crypto.bf_aweek (no int AUTO_INCREMENT UNIQUE NOT NULL,"
                              "base_no int NOT NULL,"
                              "id int NOT NULL,"
                              "price DOUBLE(9,1) NOT NULL,"
                              "timestamp DOUBLE(13,3) NOT NULL,"
                              "primary key(id),""index(no));",
    "CREATE TABLE blowupbbs_crypto.bf_amonth (no int AUTO_INCREMENT UNIQUE NOT NULL,"
                               "base_no int NOT NULL,"
                               "id int NOT NULL,"
                               "price DOUBLE(9,1) NOT NULL,"
                               "timestamp DOUBLE(13,3) NOT NULL,"
                               "primary key(id), index(no));"
]

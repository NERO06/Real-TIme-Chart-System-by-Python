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

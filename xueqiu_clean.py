# -*- coding: utf-8 -*-

import pymysql
import re

def connect_db():
    try:
        db = pymysql.connect("*********", "******", "********", "*******", charset='utf8')
        cursor = db.cursor()
        return {"db": db, "cursor": cursor}
    except:
        return {"db":"","cursor":""}


def db_close(db):
    db.close()


def query_data(cursor, query_sql):
    cursor.execute(query_sql)
    return cursor.fetchall()

def modify_data(db,cursor,modify_sql):
    cursor.execute(modify_sql)
    db.commit()

db_resp = connect_db()
db = db_resp['db']
cursor = db_resp['cursor']

query_result = query_data(cursor=cursor, query_sql="SELECT distinct(comments_text), id, mood, is_active, comments_stock from original_xueqiu_data where CHAR_LENGTH(comments_text) < 500 and comments_stock != 'SH600309' group by comments_text order by `id` limit 3600,5000")
pattern = re.compile(ur"[\u4e00-\u9fa5\uff00-\uffef]")
# print query_result[0][0]
for q in query_result:
    result = pattern.findall(q[0])
    txt = ''.join(result)
    sql = "INSERT INTO clean_xueqiu_data_short (original_id, text, mood, is_active, stock) values ('%s', '%s', '%s', '%s', '%s')" % (q[1], txt, q[2], q[3], q[4])
    print sql
    modify_data(db,cursor,sql)
# result = pattern.findall(query_result[0][0])
# txt = ''.join(result)

db_close(db)
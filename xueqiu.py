# -*- coding: utf-8 -*-
import time
import uuid
import random,requests
import json
import pymysql,logging

logger = logging.getLogger('get_xueqiu_data')
logger.setLevel(logging.DEBUG)
# 创建一个handler，用于写入日志文件
fh = logging.FileHandler('D:/get_xueqiu_data.log',encoding='UTF-8')
fh.setLevel(logging.DEBUG)
# 再创建一个handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

# stock_new = {value:key for key,value in stocks.items()}
stocks = {'SH601328': '交通银行','SH600519': '贵州茅台','SH603993': '洛阳钼业', 'SH601186': '中国铁建', 'SH601211': '国泰君安', 'SH601881': '中国银河', 'SH601688': '华泰证券', 'SH601989': '中国重工', 'SH601878': '浙商证券', 'SH601166': '兴业银行', 'SH600340': '华夏银行', 'SH600111': '北方稀土', 'SH600309': '万华化学', 'SH601088': '中国神华', 'SH601169': '北京银行', 'SH600029': '南方航空', 'SH600048': '保利地产', 'SH601288': '农业银行', 'SH601390': '中国中铁', 'SH600887': '伊利股份', 'SH600028': '中国石化', 'SH601800': '中国交建', 'SH601229': '上海银行', 'SH601398': '工商银行', 'SH600999': '招商证券', 'SH601601': '中国太保', 'SH601336': '新华保险', 'SH600016': '民生银行', 'SH600958': '东方证券', 'SH601985': '中国核电', 'SH600030': '中信证券', 'SH600050': '中国联通', 'SH601988': '中国银行', 'SH600019': '宝钢股份', 'SH600606': '绿地控股', 'SH601628': '中国人寿', 'SH601818': '光大银行', 'SH600104': '上汽集团', 'SH600036': '招商银行', 'SH601318': '中国平安', 'SH600547': '山东黄金', 'SH601857': '中国石油', 'SH601006': '大秦铁路', 'SH601766': '中国中车', 'SH601669': '中国电建', 'SH600518': '康美药业', 'SH600919': '江苏银行', 'SH601668': '中国建筑', 'SH600837': '海通证券', 'SH600000': '浦发银行' }

stocks_list=['SH601328', 'SH600999', 'SH601628', 'SH600016', 'SH601985', 'SH600019', 'SH600028', 'SH601668', 'SH601878', 'SH601688', 'SH601669', 'SH601390', 'SH601211', 'SH601006', 'SH601989', 'SH601988', 'SH600104', 'SH600518', 'SH601398', 'SH600958', 'SH600309', 'SH601857', 'SH600030', 'SH601318', 'SH600837', 'SH600036', 'SH600050', 'SH600547', 'SH601766', 'SH601166', 'SH601601', 'SH601229', 'SH600919', 'SH601169', 'SH600029', 'SH601800', 'SH600000', 'SH600887', 'SH600340', 'SH601881', 'SH601088', 'SH603993', 'SH600519', 'SH601336', 'SH601186', 'SH600606', 'SH601818', 'SH600048', 'SH600111', 'SH601288']
print(stocks_list)

def get_xueqiu_data(symbol):
    comment_list_10 = []
    ips=['121.31.159.197', '175.30.238.78', '124.202.247.110']
    url = "https://xueqiu.com/statuses/search.json"
    querystring = {"count":"10","comment":"0","symbol":symbol,"hl":"0","source":"all","sort":"","page":"1","q":""}
    headers = {
        'cache-control': "no-cache",
        'postman-token': "989379fe-9ac9-8c6f-edd0-da345f18aad4",
        'X-Forwarded-for':ips[random.randint(0, 2)],
        'user-agent': "User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
        }
    s=requests.session()
    try:
        for i in range(1,11):
            querystring['page']=i
            s.get("https://xueqiu.com/S/" + symbol, headers=headers)
            resp = s.get(url, headers=headers, params=querystring)
            comment_list=json.loads(resp.text)
            print(comment_list['list'])
            # time.sleep(3)
            comment_list_10=comment_list_10+comment_list['list']
        return comment_list_10
    except:
        logger.exception("Exception Logged")
        return comment_list_10


# comment_list_10=get_xueqiu_data("SH600340")
# print(comment_list_10)

def connect_db():
    try:
        db = pymysql.connect("45.76.77.76", "root", "123456654321", "stockMarketMoodAnalysis", charset='utf8')
        cursor = db.cursor()
        return {"db": db, "cursor": cursor}
    except:
        logger.exception("Exception Logged")
        return {"db":"","cursor":""}



def modify_data(db,cursor,modify_sql):
    try:
        cursor.execute(modify_sql)
        db.commit()
        return 1
    except:#3的语法except  2的语法是except Exception,e:  logger.info(traceback.print_exc(e))
        db.rollback()
        logger.exception("Exception Logged")
        logger.info("modify_sql: %s"%modify_sql)
        return 0
def db_close(db):
    db.close()

# modify_data('insert into original_xueqiu_data (comments_stock,comments_id) values("SH600350","123476");')
def set_None_to_zero(para):
    if para==None:
        return 0
    else:
        return para

def store_data():
    db_resp = connect_db()
    db = db_resp['db']
    cursor = db_resp['cursor']
    progress_bar="[                                                  ]"
    for stock_symbol in stocks_list:
        stock_name=stocks[stock_symbol]
        print("stock_name:%s,stock_symbol:%s"%(stock_name,stock_symbol))
        logger.info(stock_name + '(' + stock_symbol + '):\n')
        start_time=time.time()
        comment_list_10 = get_xueqiu_data(stock_symbol)
        end_time=time.time()
        print("抓取数据耗时:%r"%(end_time-start_time))
        if comment_list_10!=[]:
            try:
                for comments in comment_list_10:
                    # print(comment["text"])
                    # print("")
                    if stock_symbol+")$" in comments['text']:
                        # print(comments['text'])
                        comments_stock = stock_symbol
                        comments_id = comments['id']
                        created_at = comments['created_at']/1000
                        time_local = time.localtime(created_at)
                        comments_created_at = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
                        comments_title = comments['title']
                        comments_text = comments['text']
                        comments_retweet_count = comments['retweet_count']#转发
                        comments_like_count = comments['like_count']#点赞
                        comments_user_id = comments['user_id']
                        comments_user_screen_name = comments['user']['screen_name']
                        comments_user_friends_count = set_None_to_zero(comments['user']['friends_count'])
                        comments_user_followers_count = set_None_to_zero(comments['user']['followers_count'])
                        create_date=str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
                        insert_sql = "INSERT INTO original_xueqiu_data_copy_test (comments_stock,comments_id,comments_created_at," \
                                     "comments_title,comments_text,comments_retweet_count,comments_like_count," \
                                     "comments_user_id,comments_screen_name,comments_user_friends_count," \
                                     "comments_user_followers_count,create_date) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"\
                                     % (comments_stock,comments_id,comments_created_at,
                                     comments_title,comments_text,comments_retweet_count,comments_like_count,
                                     comments_user_id,comments_user_screen_name,comments_user_friends_count,
                                     comments_user_followers_count,create_date)

                        if db!="" and cursor!="":
                            db_start_time = time.time()
                            modify_data(db,cursor,insert_sql)
                            db_end_time = time.time()
                            print("插入数据库耗时：%r"%(db_end_time-db_start_time))
                        else:
                            logger.info("连接数据库失败，没有插入数据库")
            except:
                logger.exception("Exception Logged")
        else:
            logger.info("抓取数据为空")
        progress_bar_re=progress_bar.replace(" ", "#", 1)
        print(progress_bar_re)
    db_close(db)


print("开始时间：%r"%time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
store_data()
print("结束时间：%r"%time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))


# try:
#     print("11"+a)
# except:
#     logger.exception("Exception Logged")
    # logger.info(e.with_traceback())

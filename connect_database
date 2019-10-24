from pyhive import presto,hive
import pandas as pd
from sqlalchemy import create_engine
import urllib.request
from bs4 import BeautifulSoup as bs
import datetime


class get_data(object):
    def __init__(self,presto_user='***',presto_password='****',mysql_host='****',mysql_port,
                 mysql_user='****',mysql_password='****',mysql_database,
                 presto_sh_http,
                 presto_wj_http
                 ):
        self.presto_user = presto_user
        self.presto_password = presto_password
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_database = mysql_database
        self.presto_sh_http = presto_sh_http
        self.presto_wj_http = presto_wj_http
    def get_host(self,http_address):
        with urllib.request.urlopen(http_address) as response:
            html = response.read()
            h = bs(html, features="lxml")
        address = eval(h.get_text())['services'][0]['properties']['http']
        host = address.replace("http://", '').split(':')[0]
        port = address.replace("http://", '').split(':')[1]
        return host,port

    def trans_to_DF(self,cursor):
        columns = [datanum[0] for datanum in cursor.description]
        df = pd.DataFrame(cursor.fetchall(),columns=columns)
        return df


    def get_data_presto(self,sql_text,cluster):
        """
        :param sql_text: sql语句
        :param mode: 指定使用presto 还是 hive
        :return: pandas.DataFrame
        """
        PRESTO_SH_HOST, PRESTO_SH_PORT = self.get_host(self.presto_sh_http)
        PRESTO_WJ_HOST, PRESTO_WJ_PORT = self.get_host(self.presto_wj_http)
        if cluster == 'WJ':
            PRESTO_HOST = PRESTO_WJ_HOST
            PRESTO_PORT = PRESTO_WJ_PORT
        else:
            PRESTO_HOST = PRESTO_SH_HOST
            PRESTO_PORT = PRESTO_SH_PORT

        conn = presto.connect(host=PRESTO_HOST, port=PRESTO_PORT, username=self.presto_user)
        cursor = conn.cursor()
        cursor.execute(sql_text)
        df = self.trans_to_DF(cursor)
        cursor.close()
        print('presto success')

        return df

# 链接mysql
    def get_data_mysql(self,sql):
        engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(self.mysql_user,self.mysql_password,self.mysql_host,self.mysql_port,self.mysql_database))
        df = pd.read_sql(sql, engine)
        return df

    def iter_get_data(self,sql_path,start_date,end_date,freq,cluster,save_path):
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        print(start, end)
        getter = self.get_data()
        sql = self.get_sql(sql_path)
        date_start, date_end = datetime.datetime.strptime(start_date, '%Y-%m-%d').date(), datetime.datetime.strptime(
            start_date, '%Y-%m-%d').date()
        print(date_end, date_start)
        while date_end < end:
            delta = end - date_end
            if delta.days >= freq:
                date_end = (date_start + datetime.timedelta(days=freq))
                print(date_end)
                print(date_start, date_end)
                if date_start == start:
                    tmp = getter.get_data_presto(sql.format(start_date=date_start, end_date=date_end), cluster)
                    tmp.to_csv(save_path, mode='a', index=False)

                else:

                    tmp = getter.get_data_presto(sql.format(start_date=date_start, end_date=date_end), cluster)
                    tmp.to_csv(save_path, mode='a', header=False, index=False)

                date_start = (date_end + datetime.timedelta(days=1))
            else:
                date_end = (date_end + datetime.timedelta(days=delta.days))
                print(date_start, date_end)
                tmp = getter.get_data_presto(sql.format(start_date=date_start, end_date=date_end), cluster)
                tmp.to_csv(save_path, mode='a', header=False, index=False)

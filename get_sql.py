# -*- coding: utf-8 -*-
# @Time    : 2019-10-16 10:30
# @Author  : Evan
# @File    : get_sql.py
# @Software: PyCharm
import os

def get_sql(file_name):
    base_path = os.path.abspath(os.path.join(os.path.dirname("__file__"), os.path.pardir))
    sql_path = os.path.join(base_path, 'sql_file')
    file_path = os.path.join(sql_path,file_name)
    sql=""""""
    print(file_path)
    with open(file_path,encoding='utf-8') as f:
        for line in f.readlines():
            sql+= line
    return sql


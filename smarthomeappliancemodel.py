#! /usr/bin/env python2
# -*- coding:utf-8 -*-
# author: lisanmiao <lisanmiaoanda@126.com>

import time
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')

from mysql_helper import mysql_helper


class tableModel(mysql_helper):
    """
        table model
    """

    tableName = 'table_name'

    def __init__(self):
        """init"""
        super(tableModel, self).__init__(db_host='db_host', db_port=3323,\
                db_user='username', db_pass='passwd', db_name='db_name', charset='utf8')
  

    def get_common_condition(self):
        return "user_id not like '%debug%' and user_id not like '%monitor%' and user_id not like '%test%'"

    def get_total_user_num(self):
        """get_total_user_num"""
        fields='count(distinct user_id) total_user_num'
        sql_child_stat = {
            "where": self.get_common_condition(),
        }
        record = self.select(fields, **sql_child_stat)
        num = record[0]['total_user_num']
        return num 

    def get_new_user_id(self, from_date, to_date):
        """get_new_user_id 获取新增用户id
        from_date: 开始时间 %Y-%m-%d
        to_date: 结束时间 %Y-%m-%d
        """
        from_timestamp =  int(time.mktime(datetime.datetime.strptime(from_date, "%Y-%m-%d").timetuple()))
        to_timestamp =  int(time.mktime(datetime.datetime.strptime(to_date, "%Y-%m-%d").timetuple()))
        fields='distinct user_id'
        sql_child_stat = {
            "where": self.get_common_condition(),
            "group": 'user_id having min(create_time) > ' + str(from_timestamp) + ' and min(create_time) < ' + str(to_timestamp),
        }
        record = self.select(fields, **sql_child_stat)
        return record 

def main():
    objTableModel = tableModel()
    from_date = '2018-09-11'
    to_date = '2018-09-12'
    print objTableModel.get_new_user_id(from_date, to_date)

if __name__ == "__main__":
    main()

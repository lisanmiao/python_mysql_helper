#! /usr/bin/env python2
# -*- coding:utf-8 -*-
# author:Tiger <DropFan@Gmail.com>
# update by lisanmiao:lisanmiao<lisanmiaoanda@126.com>

import MySQLdb
import time

__author__ = 'Tiger <DropFan@Gmail.com>; lisnamiao<lisanmiaoanda@126.com>'


class mysql_helper(object):
    """
        docstring for db_mysql
        waiting for finish...
    """

    # connect config
    config = {
        'db_host': 'host_ip',
        'db_port': 3306,
        'db_user': 'username',
        'db_pass': 'passwd',
        'db_name': 'db_name',
        'charset': 'utf8',
        'read_default_file': '/etc/my.cnf'
    }

    # if auto commit() when insert/update/delete
    autocommit = False

    # connect and cursor instance of MySQLdb
    conn = None
    cur = None
    sql = ''

    def __init__(self, **kwargs):
        """
        initialize for db_mysql instance

        Args:
            **kwargs: some connect params
        """
        super(mysql_helper, self).__init__()

        for k in kwargs:
            if k in self.config.keys() and kwargs[k] != self.config[k]:
                self.config[k] = kwargs[k]

        if 'autocommit' in kwargs and isinstance(kwargs['autocommit'], bool):
            self.autocommit = kwargs['autocommit']

        for i in range(0, 10):
            self.conn = self.connect()
            if self.conn:
                self.cur = self.conn.cursor()
                break
            else:
                print 'initial connect faild. retry ' + str(i) + 'th'
                time.sleep(3)

    def connect(self, **kwargs):
        """
        connect to mysql server

        Args:
            **kwargs: some connect params
        """
        for k in kwargs:
            #__print 'connect kwargs[%s]:%s' % (k, kwargs[k])
            if k in self.config.keys() and kwargs[k] != self.config[k]:
                self.config[k] = kwargs[k]
        if self.conn:
            return self.conn
        try:
            self.conn = MySQLdb.connect(host=self.config['db_host'],
                                        port=self.config['db_port'],
                                        user=self.config['db_user'],
                                        passwd=self.config['db_pass'],
                                        db=self.config['db_name'],
                                        charset=self.config['charset'],
                                        read_default_file=self.config['read_default_file']
                                        )
            self.cur = self.conn.cursor()
            return self.conn
        except MySQLdb.Error, e:
            print 'mysql error : %s' % e
        except Exception, e:
            print 'connect error :'
            print repr(e)
        return False

    def select_db(self, db):
        """select_db

        Args:
            db (str): database name

        Returns:
            bool: true or false
        """
        if not self.is_connected():
            return False
        try:
            res = self.conn.select_db(db)
            if res:
                return True
        except Exception, e:
            print 'select_db error : %s' % e
        return False

    def charset(self, charset):
        """change charset

        Args:
            charset (str):charset

        Returns:
            bool: true or false
        """
        if not self.is_connected():
            return False
        try:
            self.cur.execute('SET NAMES %s' % charset)
            return True
        except Exception, e:
            print 'change charset error : %s' % e
        return False

    def query(self, sql):
        if not self.is_connected():
            return False
        try:
            res = self.cur.execute(sql)
            self.sql = sql
            return res
        except MySQLdb.Error, e:
            print 'MySQLdb execute error! SQL:%s\nmysql error[%d]:%s' % (sql, e[0], e[1])
        return False

    def insert(self, data):
        if not self.is_connected():
            return False
        # data = 'INSERT INTO '
        if isinstance(data, dict):
            fields = ','.join(['`%s`' % x for x in data.keys()])
            values = ','.join(["'%s'" % x for x in data.values()])
            sql = 'INSERT INTO `%s` (%s) VALUES (%s)' % (self.tableName, fields, values)
        elif isinstance(data, str):
            sql = 'INSERT INTO `%s` %s' % (self.tableName, data)
        else:
            print 'Invalid parameter type (data must be dict or string)'
            return False
        #__print 'INSERT SQL: %s' % sql
        try:
            self.cur.execute(sql)
            self.sql = sql
            insert_id = self.conn.insert_id()
            if self.autocommit:
                self.conn.commit()
            return insert_id
        except MySQLdb.Error, e:
            print 'MySQLdb insert error! SQL:%s\nmysql error[%d]:%s' % (sql, e[0], e[1])
        return False

    def update(self, data, **kwargs):
        if not self.is_connected():
            return False
        if isinstance(data, dict):
            field = ','.join("`%s`='%s'" % (k, data[k]) for k in data)

            if 'where' in kwargs and isinstance(kwargs['where'], str):
                where = kwargs['where']
            elif 'id' in data.keys():
                where = 'id = %s' % data['id']
            else:
                where = '1'

            if 'limit' in kwargs \
                and isinstance(kwargs['limit'], str) \
                    and kwargs['limit'] != '':
                limit = 'LIMIT %s' % kwargs['limit']
            else:
                limit = ''

            sql = "UPDATE `%s` SET %s WHERE %s %s" % (self.tableName, field, where, limit)
        elif isinstance(data, str):
            sql = data
        else:
            print 'Invalid parameter type (data must be dict or string)'
            return False
        #__print 'UPDATE SQL: %s' % sql
        try:
            res = self.cur.execute(sql)
            self.sql = sql
            if self.autocommit:
                self.conn.commit()
            if res:
                return self.rowcount()
        except MySQLdb.Error, e:
            print 'MySQLdb update error! SQL:%s\nmysql error[%d]:%s' % (sql, e[0], e[1])
        return False

    def delete(self, **kwargs):
        if not self.is_connected():
            return False

        if 'where' in kwargs and isinstance(kwargs['where'], str):
            where = kwargs['where']
        else:
            where = '1 = 2'

        if 'limit' in kwargs \
            and isinstance(kwargs['limit'], str) \
                and kwargs['limit'] != '':
                limit = 'LIMIT %s' % kwargs['limit']
        else:
            limit = ''

        sql = 'DELETE FROM `%s` WHERE %s %s' % (self.tableName, where, limit)
        #__print 'DELETE SQL: ', sql
        try:
            res = self.cur.execute(sql)
            self.sql = sql
            if self.autocommit:
                self.conn.commit()
            if res:
                return self.rowcount()
        except MySQLdb.Error, e:
            print 'MySQLdb delete error! SQL:%s\nmysql error[%d]:%s' % (sql, e[0], e[1])
        return False

    def select(self, fields='*', **kwargs):
        if not self.is_connected():
            return False

        if isinstance(fields, tuple) or isinstance(fields, list):
            fields = ', '.join(['`%s`' % x for x in fields])
        elif not isinstance(fields, str):
            fields = '*'

        if 'where' in kwargs and isinstance(kwargs['where'], str):
            where = 'WHERE %s' % kwargs['where']
        else: where = ''

        # group by and having condition
        if 'group' in kwargs and isinstance(kwargs['group'], str):
            group = 'GROUP BY %s' % kwargs['group']
        else:
            group = ''

        if 'order' in kwargs and isinstance(kwargs['order'], str):
            order = 'ORDER BY %s' % kwargs['order']
        else:
            order = ''

        if 'limit' in kwargs and isinstance(kwargs['limit'], str):
            limit = 'LIMIT %s' % kwargs['limit']
        else:
            limit = ''

        sql = 'SELECT %s FROM `%s` %s %s %s %s' % (fields, self.tableName, where, group, order,\
                limit)
        #__print 'SELECT SQL: ', sql
        try:
            res = self.cur.execute(sql)
            self.sql = sql
            if res:
                return self.fetch_all_dict()
        except MySQLdb.Error, e:
            print 'MySQLdb select error! SQL:%s\nmysql error[%d]:%s' % (sql, e[0], e[1])
        return False

    def rowcount(self):
        return self.cur.rowcount

    def execute(self, sql):
        self.sql = sql
        return self.cur.execute(sql)

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()

    def fetch_all(self):
        if not self.is_connected():
            return False
        try:
            res = self.cur.fetchall()
            return res
        except MySQLdb.Error, e:
            print 'MySQLdb error : %s' % e
        return False

    def fetch_all_dict(self):
        if not self.is_connected():
            return False
        try:
            res = self.cur.fetchall()
            desc = self.cur.description
            dic = []
            for row in res:
                #__print 'row', row # debug
                _dic = {}
                for i in range(0, len(row)):
                    try:
                        _dic[desc[i][0]] = str(row[i])
                    except UnicodeEncodeError, e:
                        _dic[desc[i][0]] = unicode(row[i])
                dic.append(_dic)
            return dic
        except MySQLdb.Error, e:
            print 'MySQLdb error : %s' % e
        return False

    def fetch_one(self):
        return self.cur.fetchone()

    def fetch_one_dict(self):
        if not self.is_connected():
            return False
        try:
            res = self.cur.fetchone()
            desc = self.cur.description
            dic = {}
            for i in range(0, len(res)):
                try:
                    dic[desc[i][0]] = str(res[i])
                except UnicodeEncodeError, e:
                    dic[desc[i][0]] = unicode(res[i])
            return dic
        except MySQLdb.Error, e:
            print 'MySQLdb error : %s' % e
        return False

    def close(self):
        if not self.is_connected():
            return False
        try:
            self.cur.close()
            self.conn.close()
        except Exception, e:
            print 'close error :%s' % e

    def is_connected(self):
        if self.conn:
            return True
        else:
            print 'You are not connected to mysql server!'
        return False

    def __del__(self):
        self.close()

    def get_last_sql(self):
        return self.sql

# end class mysql_helper

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import pymysql
import codecs
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from mysql_binlog_to_sql.binlog2sql_util import command_line_args, concat_sql_from_binlog_event, create_unique_file, \
    is_dml_event, event_type

SPLIT_LINE_FLAG = "##=================SPLIT==LINE=====================##"
MAX_SQL_COUNT_PER_FILE = 10000
MAX_SQL_COUNT_PER_WRITE = 1000


class Binlog2sql(object):

    def __init__(self, connection_settings, start_file=None, start_pos=None, end_file=None, end_pos=None,
                 start_time=None, stop_time=None, only_schemas=None, only_tables=None, no_pk=False,
                 flashback=False, stop_never=False, back_interval=1.0, only_dml=True, sql_type=None,
                 rollback_with_primary_key=False, rollback_with_changed_value=False):
        """
        conn_setting: {'host': 127.0.0.1, 'port': 3306, 'user': user, 'passwd': passwd, 'charset': 'utf8'}
        """

        if not start_file:
            raise ValueError('Lack of parameter: start_file')

        self.conn_setting = connection_settings
        self.start_file = start_file
        self.start_pos = start_pos if start_pos else 4  # use binlog v4
        self.end_file = end_file if end_file else start_file
        self.end_pos = end_pos
        if start_time:
            self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.datetime.strptime('1980-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        if stop_time:
            self.stop_time = datetime.datetime.strptime(stop_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.stop_time = datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")
        self.rollback_with_primary_key = rollback_with_primary_key
        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.no_pk, self.flashback, self.stop_never, self.back_interval = (no_pk, flashback, stop_never, back_interval)
        self.only_dml = only_dml
        self.sql_type = [t.upper() for t in sql_type] if sql_type else []
        self.binlogList = []
        file_name = '%s__%s' % (self.conn_setting['host'], self.conn_setting['port'])
        self.connection = pymysql.connect(**self.conn_setting)
        execute_sql_file, rollback_sql_file, tmp_sql_file = create_unique_file(file_name)
        self.execute_sql_file = execute_sql_file
        self.rollback_sql_file = rollback_sql_file
        self.tmp_sql_file = tmp_sql_file
        with self.connection as cursor:
            cursor.execute("SHOW MASTER STATUS")
            self.eof_file, self.eof_pos = cursor.fetchone()[:2]
            cursor.execute("SHOW MASTER LOGS")
            bin_index = [row[0] for row in cursor.fetchall()]
            if self.start_file not in bin_index:
                raise ValueError('parameter error: start_file %s not in mysql server' % self.start_file)
            binlog2i = lambda x: x.split('.')[1]
            for binary in bin_index:
                if binlog2i(self.start_file) <= binlog2i(binary) <= binlog2i(self.end_file):
                    self.binlogList.append(binary)

            cursor.execute("SELECT @@server_id")
            self.server_id = cursor.fetchone()[0]
            if not self.server_id:
                raise ValueError('missing server_id in %s:%s' % (self.conn_setting['host'], self.conn_setting['port']))

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.conn_setting, server_id=self.server_id,
                                    log_file=self.start_file, log_pos=self.start_pos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, resume_stream=True, blocking=True)

        flag_last_event = False
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        # to simplify code, we do not use flock for tmp_file.
        file_name = '%s__%s' % (self.conn_setting['host'], self.conn_setting['port'])
        with self.connection as cursor:
            sql_list = []
            for binlog_event in stream:
                # for attr_name in dir(binlog_event):
                #     print attr_name + ":" + str(getattr(binlog_event, attr_name))
                if not self.stop_never:
                    try:
                        event_time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
                    except OSError:
                        event_time = datetime.datetime(1980, 1, 1, 0, 0)
                    if (stream.log_file == self.end_file and stream.log_pos == self.end_pos) or \
                            (stream.log_file == self.eof_file and stream.log_pos == self.eof_pos):
                        flag_last_event = True
                    elif event_time < self.start_time:
                        if not (isinstance(binlog_event, RotateEvent)
                                or isinstance(binlog_event, FormatDescriptionEvent)):
                            last_pos = binlog_event.packet.log_pos
                        continue
                    elif (stream.log_file not in self.binlogList) or \
                            (self.end_pos and stream.log_file == self.end_file and stream.log_pos > self.end_pos) or \
                            (stream.log_file == self.eof_file and stream.log_pos > self.eof_pos) or \
                            (event_time >= self.stop_time):
                        break
                    # else:
                    #     raise ValueError('unknown binlog file or position')
                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event,
                                                       flashback=self.flashback, no_pk=self.no_pk,
                                                       rollback_with_primary_key=self.rollback_with_primary_key)
                    if sql:
                        sql_list.append(sql)
                        if len(sql_list) == MAX_SQL_COUNT_PER_WRITE:
                            self.write_tmp_sql(sql_list=sql_list)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        sql = concat_sql_from_binlog_event(
                            cursor=cursor, binlog_event=binlog_event, no_pk=self.no_pk,
                            row=row, flashback=self.flashback, e_start_pos=e_start_pos,
                            rollback_with_primary_key=self.rollback_with_primary_key)
                        sql_list.append(sql)
                        if len(sql_list) == MAX_SQL_COUNT_PER_WRITE:
                            self.write_tmp_sql(sql_list=sql_list)
                            sql_list = []

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break
            self.write_tmp_sql(sql_list=sql_list)
            stream.close()

            if self.flashback:
                self.write_rollback_sql()
            print("===============================================")
            if self.flashback:
                print("执行脚本文件：{0}".format(self.execute_sql_file))
            else:
                print("回滚脚本文件:{0}".format(self.rollback_sql_file))
            print("===============================================")
        return True

    def write_tmp_sql(self, sql_list):
        print("{0} binlog process,please wait...".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        if self.flashback:
            sql_file = self.tmp_sql_file
        else:
            sql_file = self.execute_sql_file
        with codecs.open(sql_file, "a+", 'utf-8') as f_tmp:
            for sql_item in sql_list:
                f_tmp.writelines(sql_item)
                f_tmp.writelines("\n" + SPLIT_LINE_FLAG + "\n")

    def write_rollback_sql(self):
        """print rollback sql from tmp_file"""
        with codecs.open(self.tmp_sql_file, "r", 'utf-8') as f_tmp:
            sql_item = [SPLIT_LINE_FLAG]
            sql_item_list = []
            rollback_file_id = 9999
            while True:
                lines = f_tmp.readlines(MAX_SQL_COUNT_PER_WRITE)
                if lines:
                    for line in lines:
                        if str(line).strip() == SPLIT_LINE_FLAG:
                            sql_item_list.append(sql_item)
                            sql_item = [SPLIT_LINE_FLAG]
                            if len(sql_item_list) == MAX_SQL_COUNT_PER_FILE:
                                self.write_rollback_tmp_file(rollback_file_id, sql_item_list)
                                sql_item_list = []
                                rollback_file_id -= 1
                        else:
                            sql_item.append(line)
                else:
                    break
            self.write_rollback_tmp_file(rollback_file_id, sql_item_list)

    def write_rollback_tmp_file(self, rollback_file_id, sql_item_list):
        print(
            "{0} generate rollback script,please wait...".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        tmp_rollback_sql_file = str(self.rollback_sql_file).replace("[file_id]", str(rollback_file_id))
        with codecs.open(tmp_rollback_sql_file, "w",'utf-8') as f_tmp:
            row_count = len(sql_item_list)
            for row_index in range(row_count):
                row_item = sql_item_list[row_count - row_index - 1]
                for line_item in row_item:
                    f_tmp.writelines(line_item + "\n")


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file=args.start_file, start_pos=args.start_pos,
                            end_file=args.end_file, end_pos=args.end_pos, start_time=args.start_time,
                            stop_time=args.stop_time, only_schemas=args.databases, only_tables=args.tables,
                            no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never,
                            back_interval=args.back_interval, only_dml=args.only_dml, sql_type=args.sql_type,
                            rollback_with_primary_key=args.rollback_with_primary_key)
    binlog2sql.process_binlog()
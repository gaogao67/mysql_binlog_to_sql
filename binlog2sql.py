#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import pymysql
import codecs
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from binlog2sql_util import command_line_args, concat_sql_from_binlog_event, create_unique_file, \
    is_dml_event, event_type

SPLIT_LINE_FLAG = "##=================SPLIT==LINE=====================##"
SPLIT_TRAN_FLAG = "##=================NEW=TRANSACTION=================##"
EMPTY_LINE_FLAG = "\n"
MAX_SQL_COUNT_PER_FILE = 10000
MAX_SQL_COUNT_PER_WRITE = 10000


class Binlog2sql(object):

    def __init__(self, connection_settings, start_file=None, start_pos=None, end_file=None, end_pos=None,
                 start_time=None, stop_time=None, only_schemas=None, only_tables=None, no_pk=False,
                 flashback=False, stop_never=False, back_interval=1.0, only_dml=True, sql_type=None,
                 rollback_with_primary_key=False, rollback_with_changed_value=False,
                 pseudo_thread_id=0):
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
        self.pseudo_thread_id = pseudo_thread_id
        if start_time:
            self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.datetime.strptime('1980-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        if stop_time:
            self.stop_time = datetime.datetime.strptime(stop_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.stop_time = datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")
        self.rollback_with_primary_key = rollback_with_primary_key
        self.rollback_with_changed_value = rollback_with_changed_value

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.no_pk, self.flashback, self.stop_never, self.back_interval = (no_pk, flashback, stop_never, back_interval)
        self.only_dml = only_dml
        self.sql_type = [t.upper() for t in sql_type] if sql_type else []
        self.binlogList = []
        file_name = '%s_%s' % (self.conn_setting['host'], self.conn_setting['port'])
        self.connection = pymysql.connect(**self.conn_setting)
        execute_sql_file, rollback_sql_file, tmp_sql_file = create_unique_file(file_name)
        self.execute_sql_file = execute_sql_file
        self.rollback_sql_file = rollback_sql_file
        self.tmp_sql_file = tmp_sql_file
        self.rollback_sql_files = list()
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
        slave_proxy_id = 0
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        self.touch_tmp_sql_file()
        # to simplify code, we do not use flock for tmp_file.
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
                    slave_proxy_id = binlog_event.slave_proxy_id
                    sql_list.append(SPLIT_TRAN_FLAG)
                if self.pseudo_thread_id > 0:
                    if self.pseudo_thread_id != slave_proxy_id:
                        continue
                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    sql = concat_sql_from_binlog_event(
                        cursor=cursor, binlog_event=binlog_event,
                        flashback=self.flashback, no_pk=self.no_pk,
                        rollback_with_primary_key=self.rollback_with_primary_key,
                        rollback_with_changed_value=self.rollback_with_changed_value)
                    if sql:
                        sql_list.append(sql)
                        if len(sql_list) == MAX_SQL_COUNT_PER_WRITE:
                            self.write_tmp_sql(sql_list=sql_list)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        sql = concat_sql_from_binlog_event(
                            cursor=cursor, binlog_event=binlog_event, no_pk=self.no_pk,
                            row=row, flashback=self.flashback, e_start_pos=e_start_pos,
                            rollback_with_primary_key=self.rollback_with_primary_key,
                            rollback_with_changed_value=self.rollback_with_changed_value)
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
                self.create_rollback_sql()
            else:
                self.create_execute_sql()
            print("===============================================")
            if not self.flashback:
                print("执行脚本文件：\n{0}".format(self.execute_sql_file))
            else:
                print("回滚脚本文件:")
                new_file_list = list(reversed(self.rollback_sql_files))
                for tmp_file in new_file_list:
                    print(tmp_file)
            print("===============================================")
        return True

    def touch_tmp_sql_file(self):
        """
        创建临时文件并写入第一条事务标志和第一条分隔符
        :return:
        """
        with codecs.open(self.tmp_sql_file, "a+", 'utf-8') as f_tmp:
            f_tmp.writelines(SPLIT_TRAN_FLAG + EMPTY_LINE_FLAG)
            f_tmp.writelines(SPLIT_LINE_FLAG + EMPTY_LINE_FLAG)

    def touch_rollback_sub_file(self, rollback_file_id):
        """
        创建临时文件并写入第一条事务标志和第一条分隔符
        :return:
        """
        tmp_rollback_sql_file = str(self.rollback_sql_file).replace("[file_id]", str(rollback_file_id))
        with codecs.open(tmp_rollback_sql_file, "a+", 'utf-8') as f_tmp:
            print("touch file")
            f_tmp.writelines(SPLIT_TRAN_FLAG + EMPTY_LINE_FLAG)

    def write_tmp_sql(self, sql_list):
        """
        批量将缓存的SQL脚本写入到临时文件，两条SQL之间使用分隔符分割
        :param sql_list: 
        :return: 
        """""
        print("{0} binlog process,please wait...".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        with codecs.open(self.tmp_sql_file, "a+", 'utf-8') as f_tmp:
            for sql_item in sql_list:
                if self.get_sql_count(sql_item) > 0:
                    f_tmp.writelines(sql_item)
                    f_tmp.writelines(EMPTY_LINE_FLAG + SPLIT_LINE_FLAG + EMPTY_LINE_FLAG)

    def create_execute_sql(self):
        """
        根据临时文件创建回滚脚本
        :return:
        """
        with codecs.open(self.tmp_sql_file, "r", 'utf-8') as f_tmp:
            sql_item_list = []
            while True:
                lines = f_tmp.readlines(MAX_SQL_COUNT_PER_WRITE)
                sql_item = []
                if lines:
                    for line in lines:
                        if str(line).find(SPLIT_LINE_FLAG) >= 0:
                            if self.get_sql_count(sql_item) > 0:
                                sql_item_list.append(sql_item)
                                sql_item = []
                            if len(sql_item_list) == MAX_SQL_COUNT_PER_FILE:
                                self.write_execute_file(sql_item_list)
                                sql_item_list = []
                        else:
                            sql_item.append(line)
                else:
                    break
            self.write_execute_file(sql_item_list)

    def write_execute_file(self, sql_list):
        with codecs.open(self.execute_sql_file, "a+", 'utf-8') as f_tmp:
            has_sql = True
            for sql_item in sql_list:
                for sql_line in sql_item:
                    if sql_line.find(SPLIT_TRAN_FLAG) >= 0:
                        if has_sql:
                            f_tmp.writelines(sql_line)
                            has_sql = False
                    else:
                        has_sql = True
                        f_tmp.writelines(sql_line)

    def get_sql_count(self, row_item: list):
        """
        检查一组SQL中是否包含SQL
        """
        sql_count = 0
        for line_item in row_item:
            if str(line_item).find(SPLIT_TRAN_FLAG) >= 0:
                pass
            elif str(line_item).find(SPLIT_LINE_FLAG) >= 0:
                pass
            elif str(line_item).strip() == "":
                pass
            else:
                sql_count += 1
        return sql_count

    def create_rollback_sql(self):
        """
        根据临时文件创建回滚脚本
        :return:
        """
        with codecs.open(self.tmp_sql_file, "r", 'utf-8') as f_tmp:
            sql_item = [SPLIT_LINE_FLAG]
            sql_item_list = []
            rollback_file_id = 9999
            while True:
                lines = f_tmp.readlines(MAX_SQL_COUNT_PER_WRITE)
                if lines:
                    for line in lines:
                        if str(line).find(SPLIT_LINE_FLAG) >= 0 or str(line).find(SPLIT_TRAN_FLAG) >= 0:
                            # 只有包含SQL语句的记录才会呗
                            if self.get_sql_count(sql_item) > 0:
                                sql_item_list.append(sql_item)
                            sql_item = [SPLIT_LINE_FLAG, EMPTY_LINE_FLAG]
                            if str(line).find(SPLIT_TRAN_FLAG) >= 0:
                                sql_item.append(SPLIT_TRAN_FLAG)
                            if len(sql_item_list) == MAX_SQL_COUNT_PER_FILE:
                                self.touch_rollback_sub_file(rollback_file_id)
                                self.write_rollback_sub_file(rollback_file_id, sql_item_list)
                                sql_item_list = []
                                rollback_file_id -= 1
                        else:
                            sql_item.append(line)
                else:
                    break
            self.touch_rollback_sub_file(rollback_file_id)
            self.write_rollback_sub_file(rollback_file_id, sql_item_list)

    def write_rollback_sub_file(self, rollback_file_id, sql_item_list):
        """
        将拆分后的回滚SQL列表写入指定文件号的回滚文件
        :param rollback_file_id:
        :param sql_item_list:
        :return:
        """
        print(
            "{0} generate rollback script,please wait...".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        tmp_rollback_sql_file = str(self.rollback_sql_file).replace("[file_id]", str(rollback_file_id))
        with codecs.open(tmp_rollback_sql_file, "a+", 'utf-8') as f_tmp:
            row_count = len(sql_item_list)
            is_start_info = True
            start_info = ""
            next_start_info = ""
            has_sql = True
            for row_index in range(row_count):
                row_item = sql_item_list[row_count - row_index - 1]
                for line_item in row_item:
                    if line_item.startswith("### start"):
                        if is_start_info is True:
                            start_info = line_item
                            is_start_info = False
                        next_start_info = line_item
                        f_tmp.writelines(line_item)
                    elif line_item.find(SPLIT_LINE_FLAG) >= 0:
                        pass
                    elif line_item.find(SPLIT_TRAN_FLAG) >= 0:
                        if has_sql:
                            f_tmp.writelines(line_item)
                            has_sql = False
                    else:
                        f_tmp.writelines(line_item)
                        has_sql = True
            self.write_rollback_info_file(
                tmp_rollback_sql_file=tmp_rollback_sql_file,
                start_info=start_info,
                next_start_info=next_start_info
            )
            self.rollback_sql_files.append(tmp_rollback_sql_file)

    def write_rollback_info_file(self, tmp_rollback_sql_file, start_info, next_start_info):
        """
        将拆分后的回滚文件信息写入回滚索引文件
        :param tmp_rollback_sql_file:
        :param start_info:
        :param next_start_info:
        :return:
        """
        info_rollback_sql_file = str(self.rollback_sql_file).replace("[file_id]", "index")
        with codecs.open(info_rollback_sql_file, "a+", 'utf-8') as f_tmp:
            f_tmp.writelines(SPLIT_LINE_FLAG + EMPTY_LINE_FLAG)
            f_tmp.writelines("### file path: {0}".format(tmp_rollback_sql_file) + EMPTY_LINE_FLAG)
            start_info = start_info.replace("###", "### first sql : ")
            f_tmp.writelines(start_info)
            end_info = next_start_info.replace("###", "### last sql  : ")
            f_tmp.writelines(end_info)


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file=args.start_file, start_pos=args.start_pos,
                            end_file=args.end_file, end_pos=args.end_pos, start_time=args.start_time,
                            stop_time=args.stop_time, only_schemas=args.databases, only_tables=args.tables,
                            no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never,
                            back_interval=args.back_interval, only_dml=args.only_dml, sql_type=args.sql_type,
                            rollback_with_primary_key=args.rollback_with_primary_key,
                            rollback_with_changed_value=args.rollback_with_changed_value,
                            pseudo_thread_id=args.pseudo_thread_id)
    binlog2sql.process_binlog()

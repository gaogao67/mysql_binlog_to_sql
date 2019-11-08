#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
import getpass
import json
from contextlib import contextmanager
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)

if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False


def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except Exception as ex:
        print(str(ex))
        return False


def create_unique_file(filename):
    filename = str(filename).replace(",", "_").replace("-", "_")
    dt_string = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base_dir, "log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    execute_sql_file = os.path.join(log_dir, "{0}_{1}_executed.txt".format(filename, dt_string))
    rollback_sql_file = os.path.join(log_dir, "{0}_{1}_rollback_[file_id].txt".format(filename, dt_string))
    tmp_sql_file = os.path.join(log_dir, "{0}_{1}_tmp.txt".format(filename, dt_string))
    return execute_sql_file, rollback_sql_file, tmp_sql_file


def parse_args():
    """parse args for binlog2sql"""

    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str, nargs='*',
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    interval = parser.add_argument_group('interval filter')
    interval.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    interval.add_argument('--start-position', '--start-pos', dest='start_pos', type=int,
                          help='Start position of the --start-file', default=4)
    interval.add_argument('--stop-file', '--end-file', dest='end_file', type=str,
                          help="Stop binlog file to be parsed. default: '--start-file'", default='')
    interval.add_argument('--stop-position', '--end-pos', dest='end_pos', type=int,
                          help="Stop position. default: latest position of '--stop-file'", default=0)
    interval.add_argument('--start-datetime', dest='start_time', type=str,
                          help="Start time. format %%Y-%%m-%%d %%H:%%M:%%S", default='')
    interval.add_argument('--stop-datetime', dest='stop_time', type=str,
                          help="Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;", default='')
    parser.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                        help="Continuously parse binlog. default: stop at the latest event when you start.")
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')

    event = parser.add_argument_group('type filter')
    event.add_argument('--only-dml', dest='only_dml', action='store_true', default=False,
                       help='only print dml, ignore ddl')
    event.add_argument('--sql-type', dest='sql_type', type=str, nargs='*', default=['INSERT', 'UPDATE', 'DELETE'],
                       help='Sql type you want to process, support INSERT, UPDATE, DELETE.')

    # exclusive = parser.add_mutually_exclusive_group()
    parser.add_argument('-K', '--no-primary-key', dest='no_pk', action='store_true',
                        help='Generate insert sql without primary key if exists', default=False)
    parser.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                        help='Flashback data to start_position of start_file', default=False)
    parser.add_argument('--rollback-with-primary-key', dest='rollback_with_primary_key', action='store_true',
                        help='Generate UPDATE/DELETE statement with primary key', default=False)
    parser.add_argument('--rollback-with-changed_value', dest='rollback_with_changed_value', action='store_true',
                        help='Generate UPDATE statement with changed value', default=False)
    parser.add_argument('--back-interval', dest='back_interval', type=float, default=1.0,
                        help="Sleep time between chunks of 1000 rollback sql. set it to 0 if do not need sleep")
    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.start_file:
        raise ValueError('Lack of parameter: start_file')
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    if (args.start_time and not is_valid_datetime(args.start_time)) or \
            (args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.password:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]
    return args


def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return value.decode('utf-8')
    else:
        return value


def is_dml_event(event):
    if isinstance(event, WriteRowsEvent) or isinstance(event, UpdateRowsEvent) or isinstance(event, DeleteRowsEvent):
        return True
    else:
        return False


def event_type(event):
    t = None
    if isinstance(event, WriteRowsEvent):
        t = 'INSERT'
    elif isinstance(event, UpdateRowsEvent):
        t = 'UPDATE'
    elif isinstance(event, DeleteRowsEvent):
        t = 'DELETE'
    return t


def concat_sql_from_binlog_event(cursor, binlog_event, row=None, e_start_pos=None, flashback=False,
                                 no_pk=False, rollback_with_primary_key=False):
    if flashback and no_pk:
        raise ValueError('only one of flashback or no_pk can be True')
    if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent)
            or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
        raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = ''
    if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) \
            or isinstance(binlog_event, DeleteRowsEvent):
        pattern = generate_sql_pattern(
            binlog_event, row=row, flashback=flashback,
            no_pk=no_pk, rollback_with_primary_key=rollback_with_primary_key)
        new_values_list = []
        for value_item in pattern['values']:
            if type(value_item) == list:
                # print(type(value_item))
                # print(value_item)
                new_value_item = format_list_bytes(list_item=value_item)
                new_values_list.append(json.dumps(new_value_item))
            elif type(value_item) == dict:
                new_value_item = format_dict_bytes(dict_item=value_item)
                new_values_list.append(json.dumps(new_value_item))
            elif type(value_item) == str:
                new_values_list.append(value_item)
            else:
                new_values_list.append(value_item)
        pattern['values'] = new_values_list
        sql = sql + cursor.mogrify(pattern['template'], pattern['values'])
        time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
        sql = '### start %s end %s time %s' % (e_start_pos, binlog_event.packet.log_pos, time) + '\n' + sql
    elif flashback is False and isinstance(binlog_event, QueryEvent) and binlog_event.query != 'BEGIN' \
            and binlog_event.query != 'COMMIT':
        if binlog_event.schema:
            sql = 'USE {0};\n'.format(binlog_event.schema)
        sql += '{0};'.format(fix_object(binlog_event.query))
    return sql


def format_list_bytes(list_item):
    new_list = []
    for sub_item in list_item:
        if type(sub_item) == bytes:
            new_sub_item = str(sub_item, "utf8")
        elif type(sub_item) == dict:
            new_sub_item = format_dict_bytes(sub_item)
        elif type(sub_item) == list:
            new_sub_item = format_list_bytes(sub_item)
        else:
            new_sub_item = sub_item
        new_list.append(new_sub_item)
    return new_list


def format_dict_bytes(dict_item):
    new_dict = dict()
    for sub_item in dict_item.items():
        item_key, item_value = sub_item
        if type(item_value) == bytes:
            new_item_value = str(item_value, "utf8")
        elif type(item_value) == dict:
            new_item_value = format_dict_bytes(item_value)
        elif type(item_value) == list:
            new_item_value = format_list_bytes(item_value)
        else:
            new_item_value = item_value

        if type(item_key) == bytes:
            new_item_key = str(item_key, "utf8")
        else:
            new_item_key = item_key
        new_dict[new_item_key] = new_item_value
    return new_dict


def generate_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False, rollback_with_primary_key=False):
    template = ''
    values = []
    if flashback is True:
        if (binlog_event.primary_key is None) or (not rollback_with_primary_key):
            if isinstance(binlog_event, WriteRowsEvent):
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                    binlog_event.schema, binlog_event.table,
                    ' AND '.join(map(compare_items, row['values'].items()))
                )
                values = map(fix_object, row['values'].values())
            elif isinstance(binlog_event, DeleteRowsEvent):
                template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
                values = map(fix_object, row['values'].values())
            elif isinstance(binlog_event, UpdateRowsEvent):
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                    ' AND '.join(map(compare_items, row['after_values'].items())))
                values = map(fix_object, list(row['before_values'].values()) + list(row['after_values'].values()))
        else:
            primary_key_list = []
            if type(binlog_event.primary_key) == tuple:
                for primary_key_item in binlog_event.primary_key:
                    primary_key_list.append(str(primary_key_item))
            else:
                primary_key_list.append(str(binlog_event.primary_key))
            primary_key_dict = dict()
            if isinstance(binlog_event, WriteRowsEvent):
                ## 将DELETE 修改为按照主键操作
                for primary_key_item in primary_key_list:
                    primary_key_dict[primary_key_item] = row['values'][primary_key_item]
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                    binlog_event.schema, binlog_event.table,
                    ' AND '.join(map(compare_items, primary_key_dict.items()))
                )
                values = map(fix_object, primary_key_dict.values())
            elif isinstance(binlog_event, DeleteRowsEvent):
                template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
                values = map(fix_object, row['values'].values())
            elif isinstance(binlog_event, UpdateRowsEvent):
                for primary_key_item in primary_key_list:
                    primary_key_dict[primary_key_item] = row['after_values'][primary_key_item]
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                    ' AND '.join(map(compare_items, primary_key_dict.items())))
                values = map(fix_object, list(row['before_values'].values()) + list(primary_key_dict.values()))
    else:
        if isinstance(binlog_event, WriteRowsEvent):
            if no_pk:
                # print binlog_event.__dict__
                # tableInfo = (binlog_event.table_map)[binlog_event.table_id]
                # if tableInfo.primary_key:
                #     row['values'].pop(tableInfo.primary_key)
                if binlog_event.primary_key:
                    row['values'].pop(binlog_event.primary_key)

            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                ' AND '.join(map(compare_items, row['before_values'].items()))
            )
            values = map(fix_object, list(row['after_values'].values()) + list(row['before_values'].values()))

    return {'template': template, 'values': list(values)}

# def reversed_lines(fin):
#     """Generate the lines of file in reverse order."""
#     part = ''
#     for block in reversed_blocks(fin):
#         print(type(block))
#         print(block)
#         block = str(block).encode("utf-8")
#         print(block)
#         for c in reversed(block):
#             if c == '\n' and part:
#                 yield part[::-1]
#                 part = ''
#                 print(c)
#             part += chr(c)
#     if part:
#         yield part[::-1]

# def reversed_blocks(fin, block_size=4096):
#     """Generate blocks of file's contents in reverse order."""
#     fin.seek(0, os.SEEK_END)
#     here = fin.tell()
#     while 0 < here:
#         delta = min(block_size, here)
#         here -= delta
#         fin.seek(here, os.SEEK_SET)
#         yield fin.read(delta)

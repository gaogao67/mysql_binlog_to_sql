# -*- coding: utf-8 -*-


import os
import sys
import argparse
import datetime
import getpass
import json
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)


class SQLPatternHelper(object):
    @staticmethod
    def fix_object(value):
        """Fixes python objects so that they can be properly inserted into SQL queries"""
        if isinstance(value, set):
            value = ','.join(value)
        if isinstance(value, bytes):
            return value.decode('utf-8')
        else:
            return value

    @staticmethod
    def compare_items(items):
        # caution: if v is NULL, may need to process
        (k, v) = items
        if v is None:
            return '`%s` IS %%s' % k
        else:
            return '`%s`=%%s' % k


class SqlExecutePattern(object):
    def __init__(self, binlog_event,
                 row=None,
                 flashback=False,
                 no_pk=False,
                 rollback_with_primary_key=False,
                 rollback_with_changed_value=False):
        self.binlog_event = binlog_event
        self.row = row
        self.flashback = flashback
        self.no_pk = no_pk
        self.rollback_with_primary_key = rollback_with_primary_key
        self.rollback_with_changed_value = rollback_with_changed_value

    def get_sql_pattern(self):
        if isinstance(self.binlog_event, DeleteRowsEvent):
            return self.get_delete_pattern()
        elif isinstance(self.binlog_event, UpdateRowsEvent):
            return self.get_update_pattern()
        elif isinstance(self.binlog_event, WriteRowsEvent):
            return self.get_insert_pattern()
        else:
            return None

    def fix_object(self, value):
        return SQLPatternHelper.fix_object(value)

    def compare_items(self, items):
        return SQLPatternHelper.compare_items(items)

    def get_insert_pattern(self):
        if self.no_pk and self.binlog_event.primary_key:
            self.row['values'].pop(self.binlog_event.primary_key)
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            self.binlog_event.schema, self.binlog_event.table,
            ', '.join(map(lambda key: '`%s`' % key, self.row['values'].keys())),
            ', '.join(['%s'] * len(self.row['values']))
        )
        values = map(self.fix_object, self.row['values'].values())
        return {'template': template, 'values': list(values)}

    def get_update_pattern(self):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
            self.binlog_event.schema, self.binlog_event.table,
            ', '.join(['`%s`=%%s' % k for k in self.row['after_values'].keys()]),
            ' AND '.join(map(self.compare_items, self.row['before_values'].items()))
        )
        values = map(
            self.fix_object,
            list(self.row['after_values'].values())
            + list(self.row['before_values'].values())
        )
        return {'template': template, 'values': list(values)}

    def get_delete_pattern(self):
        template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
            self.binlog_event.schema,
            self.binlog_event.table,
            ' AND '.join(map(self.compare_items, self.row['values'].items()))
        )
        values = map(self.fix_object, self.row['values'].values())
        return {'template': template, 'values': list(values)}


class SqlRollbackPattern(object):
    def __init__(self, binlog_event,
                 row=None,
                 flashback=False,
                 no_pk=False,
                 rollback_with_primary_key=False,
                 rollback_with_changed_value=False):
        self.binlog_event = binlog_event
        self.row = row
        self.flashback = flashback
        self.no_pk = no_pk
        self.rollback_with_primary_key = rollback_with_primary_key
        self.rollback_with_changed_value = rollback_with_changed_value

    def get_sql_pattern(self):
        if isinstance(self.binlog_event, DeleteRowsEvent):
            return self.get_delete_pattern()
        elif isinstance(self.binlog_event, UpdateRowsEvent):
            return self.get_update_pattern()
        elif isinstance(self.binlog_event, WriteRowsEvent):
            return self.get_insert_pattern()
        else:
            return None

    def fix_object(self, value):
        return SQLPatternHelper.fix_object(value)

    def compare_items(self, items):
        return SQLPatternHelper.compare_items(items)

    def get_diff_items(self):
        diff_items = dict()
        before_items = self.row['before_values']
        after_items = self.row['after_values']
        for item_key in before_items.keys():
            if before_items[item_key] != after_items[item_key]:
                diff_items[item_key] = before_items[item_key]
        return diff_items

    def get_primary_key_list(self):
        primary_key_list = []
        if type(self.binlog_event.primary_key) == tuple:
            for primary_key_item in self.binlog_event.primary_key:
                primary_key_list.append(str(primary_key_item))
        else:
            primary_key_list.append(str(self.binlog_event.primary_key))
        return primary_key_list

    def get_insert_pattern(self):
        if (self.rollback_with_primary_key is True) and (self.binlog_event.primary_key is not None):
            primary_key_dict = dict()
            for primary_key_item in self.get_primary_key_list():
                primary_key_dict[primary_key_item] = self.row['values'][primary_key_item]
            where_items = primary_key_dict
        else:
            where_items = self.row['values']
        template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
            self.binlog_event.schema, self.binlog_event.table,
            ' AND '.join(map(self.compare_items, where_items.items()))
        )
        values = map(self.fix_object, where_items.values())
        return {'template': template, 'values': list(values)}

    def get_update_pattern(self):
        if self.rollback_with_changed_value is True:
            self.rollback_with_primary_key = True
            update_items = self.get_diff_items()
        else:
            update_items = self.row['before_values']

        if (self.rollback_with_primary_key is True) and (self.binlog_event.primary_key is not None):
            primary_key_dict = dict()
            for primary_key_item in self.get_primary_key_list():
                primary_key_dict[primary_key_item] = self.row['after_values'][primary_key_item]
            where_items = primary_key_dict
        else:
            where_items = self.row['after_values']
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
            self.binlog_event.schema, self.binlog_event.table,
            ', '.join(['`%s`=%%s' % x for x in update_items.keys()]),
            ' AND '.join(map(self.compare_items, where_items.items())))
        values = map(self.fix_object, list(update_items.values()) + list(where_items.values()))
        return {'template': template, 'values': list(values)}

    def get_delete_pattern(self):
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            self.binlog_event.schema, self.binlog_event.table,
            ', '.join(map(lambda key: '`%s`' % key, self.row['values'].keys())),
            ', '.join(['%s'] * len(self.row['values']))
        )
        values = map(self.fix_object, self.row['values'].values())
        return {'template': template, 'values': list(values)}


class RowValueFormatter(object):
    @staticmethod
    def format_object(object_item):
        if type(object_item) == bytes:
            return RowValueFormatter.format_bytes_value(object_item)
        elif type(object_item) == dict:
            return RowValueFormatter.format_dict_value(object_item)
        elif type(object_item) == list:
            return RowValueFormatter.format_list_value(object_item)
        else:
            return object_item

    @staticmethod
    def format_row_value(row_value):
        new_row_value = RowValueFormatter.format_object(row_value)
        if type(new_row_value) == dict:
            return json.dumps(new_row_value)
        elif type(new_row_value) == list:
            return json.dumps(new_row_value)
        else:
            return new_row_value

    @staticmethod
    def format_bytes_value(bytes_item):
        return str(bytes_item, "utf8")

    @staticmethod
    def format_dict_value(dict_item):
        new_dict = dict()
        for sub_item in dict_item.items():
            item_key, item_value = sub_item
            new_item_value = RowValueFormatter.format_object(item_value)
            new_item_key = RowValueFormatter.format_object(item_key)
            new_dict[new_item_key] = new_item_value
        return new_dict

    @staticmethod
    def format_list_value(list_item):
        new_list = []
        for sub_item in list_item:
            new_sub_item = RowValueFormatter.format_object(sub_item)
            new_list.append(new_sub_item)
        return new_list

基于Python3+binlog2sql改写。

# 彻底抛弃Python2,中文乱码问题太糟心

# 优化flaskback过程，将回滚语句拆分成多个小文件倒序存放。
```
## 生产文件效果，编号越小的文件中包含的SQL执行时间越晚。
192.168.199.194_3358_20191110122331_rollback_9990.sql
192.168.199.194_3358_20191110122331_rollback_9991.sql
192.168.199.194_3358_20191110122331_rollback_9992.sql
192.168.199.194_3358_20191110122331_rollback_9993.sql
192.168.199.194_3358_20191110122331_rollback_9994.sql
192.168.199.194_3358_20191110122331_rollback_9995.sql
192.168.199.194_3358_20191110122331_rollback_9996.sql
192.168.199.194_3358_20191110122331_rollback_9997.sql
192.168.199.194_3358_20191110122331_rollback_9998.sql
192.168.199.194_3358_20191110122331_rollback_9999.sql
192.168.199.194_3358_20191110122331_index.sql
192.168.199.194_3358_20191110122331_tmp.sql
```
index.sql文件中包含每个小文件中包含的SQL开始和结束信息:
```
##=================SPLIT==LINE=====================##
### file path: I:\log\192.168.199.194_3358_20191110223556_rollback_9999.sql
### first sql :  start 279144 end 525522 time 2019-11-04 00:13:30
### last sql  :  start 429 end 8827 time 2019-11-03 23:43:55
##=================SPLIT==LINE=====================##
### file path: I:\log\192.168.199.194_3358_20191110223556_rollback_9998.sql
### first sql :  start 559232 end 805590 time 2019-11-04 00:13:40
### last sql  :  start 279144 end 525522 time 2019-11-04 00:13:30
##=================SPLIT==LINE=====================##
### file path: I:\log\192.168.199.194_3358_20191110223556_rollback_9997.sql
### first sql :  start 839300 end 1085678 time 2019-11-04 00:13:42
### last sql  :  start 559232 end 805590 time 2019-11-04 00:13:40
```
# 新增参数rollback-with-primary-key，回滚语句中WHERE部分仅包含主键
未设置rollback-with-primary-key参数生成的回滚语句为：
```
##=================SPLIT==LINE=====================##
### start 279144 end 525522 time 2019-11-04 00:13:30
DELETE FROM `db002`.`tb001_01` WHERE `ID`=7646 AND `C1`=7646 AND `DT`='2019-10-27 22:18:20' AND `C2`=7646 AND `c3` IS NULL AND `c5` IS NULL LIMIT 1;
```
设置rollback-with-primary-key参数生成的回滚语句为：
```
##=================SPLIT==LINE=====================##
### start 279144 end 525522 time 2019-11-04 00:13:30
DELETE FROM `db002`.`tb001_01` WHERE `ID`=7646 LIMIT 1;
```


# 新增参数rollback-with-changed-value，对于UPDATE类型回滚语句中SET部分仅包含数据变更过的字段。

未设置rollback-with-changed-value参数生成的回滚语句为：
```
##=================SPLIT==LINE=====================##
### start 429 end 8827 time 2019-11-03 23:43:55
UPDATE `db002`.`tb001` SET `ID`=20, `C1`=20, `DT`='2019-11-03 23:34:04', `C2`=20, `c3`='[1,2,3,4]', `c5`=NULL WHERE `ID`=20 AND `C1`=20 AND `DT`='2019-11-03 23:43:55' AND `C2`=20 AND `c3`='[1,2,3,4]' AND `c5`='[1, 2, 3, 4]' LIMIT 1;
```
设置rollback-with-primary-key参数生成的回滚语句为：
```
##=================SPLIT==LINE=====================##
### start 429 end 8827 time 2019-11-03 23:43:55
UPDATE `db002`.`tb001` SET `DT`='2019-11-03 23:34:04', `c5`=NULL WHERE `ID`=20 LIMIT 1;
```
## 新增参数pseudo-thread-id,限制导出指定thread_id的事件。

## 用法
```
## 回滚DELETE操作
python3 binlog2sql.py 
--host="mysql_host" \
--port=3306 \
--user="user_name" \
--password="user_password" \
--start-file="mysql-bin.000005" \
--start-position=746318512  \
--databases "database_name" \
--tables "table_name1" "table_name2"
--start-datetime="2020-03-28 18:50:00" \
--end-datetime="2020-03-28 18:50:00" \
--only-dml \
--sql-type="DELETE" \
--flashback 
```

## 相关问题
错误消息：
```SQL
log event entry exceeded max_allowed_packet; Increase max_allowed_packet on master; 
```
根据上述错误，提示需要修改主库max_allowed_packet的参数，但实际是传入参数(--stop-position和--start-position)有问题导致，如无法确认位点，可以通过时间点来限制

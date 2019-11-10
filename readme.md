基于binlog2sql改写。

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
192.168.199.194_3358_20191110122331_tmp.sql

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

未设置rollback-with-primary-key参数生成的回滚语句为：
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
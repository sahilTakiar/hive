--! qt:dataset:src

set hive.spark.client.type=SPARK_LAUNCHER_CLIENT;

-- Hack to restart the HoS session
set hive.execution.engine=mr;
set hive.execution.engine=spark;

explain select key, count(*) from src group by key order by key limit 100;
select key, count(*) from src group by key order by key limit 100;

DROP TABLE qtest;
CREATE TABLE qtest (value int) LOCATION '${hiveconf:test.blobstore.path.unique}/qtest/';
EXPLAIN EXTENDED INSERT INTO qtest VALUES (1), (10), (100), (1000);
INSERT INTO qtest VALUES (1), (10), (100), (1000);
EXPLAIN EXTENDED SELECT * FROM qtest;
SELECT * FROM qtest;

@Drop
DROP TABLE t1;
commit;

@Create
CREATE TABLE t1 (
  t1s VARCHAR(20)
);
commit;

@Insert
-- Simple comment
INSERT INTO t1(t1s) VALUES ('TEST');

BEGIN
 INSERT INTO t1(t1s) VALUES ('TEST2');
 INSERT INTO t1(t1s) VALUES ('TEST3');
END

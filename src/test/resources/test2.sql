@Test
CREATE TABLE t1 (
  t1s VARCHAR(20)
) \
commit \
INSERT INTO t1(t1s) VALUES ('TEST') \
commit \

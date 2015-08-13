--
--Testing cms_topn_add function of the extension 
--

--check null cms_topn
SELECT cms_topn_add(NULL, 5);

--check null new item for different type of elements
CREATE TABLE add_test (
	cms_topn_column cms_topn
);

--fixed size type like integer
INSERT INTO add_test VALUES(cms_topn(3));
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, NULL::integer);
SELECT cms_topn_info(cms_topn_column) FROM add_test;

--variable length type like text
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms_topn(3));
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, NULL::text);
SELECT cms_topn_info(cms_topn_column) FROM add_test;

--check type consistency
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms_topn(2));
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'hello'::text);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 3);

--check normal insertion
--for fixed size type like big integer
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms_topn(2));
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 0::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, NULL::bigint);
SELECT topn(cms_topn_column, NULL::bigint) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 5341434213434443::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 5341434213434443::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 5341434213434443::bigint);
SELECT topn(cms_topn_column, NULL::bigint) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -8::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -8::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -8::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -8::bigint);
SELECT topn(cms_topn_column, NULL::bigint) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -42184342348342834::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -42184342348342834::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -42184342348342834::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -42184342348342834::bigint);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, -42184342348342834::bigint);
SELECT topn(cms_topn_column, NULL::bigint) from add_test;
SELECT cms_topn_info(cms_topn_column) from add_test;

--for variable length type like text
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms_topn(2));
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'hello'::text);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'hello'::text);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'hello'::text);
SELECT topn(cms_topn_column, NULL::text) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'world'::text);
SELECT topn(cms_topn_column, NULL::text) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, NULL::text);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, NULL::text);
SELECT topn(cms_topn_column, NULL::text) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'cms_topn'::text);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'cms_topn'::text);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'cms_topn'::text);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, 'cms_topn'::text);
SELECT topn(cms_topn_column, NULL::text) from add_test;


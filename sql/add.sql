--
--Testing cms_topn_add function of the extension 
--

--check null cms_topn
SELECT cms_topn_add(NULL, 5);

--check composite types
create type composite_type as ( a int, b text);
select cms_topn_add(cms_topn(2), (3,'cms_topn')::composite_type);

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
--cidr
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms_topn(2));
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.100.128/25'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, NULL::cidr);
SELECT topn(cms_topn_column, NULL::cidr) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168/24'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168/24'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168/24'::cidr);
SELECT topn(cms_topn_column, NULL::cidr) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168/25'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168/25'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168/25'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168/25'::cidr);
SELECT topn(cms_topn_column, NULL::cidr) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.1'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.1'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.1'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.1'::cidr);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.1'::cidr);
SELECT topn(cms_topn_column, NULL::cidr) from add_test;
SELECT cms_topn_info(cms_topn_column) from add_test;

--inet
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms_topn(2));
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.100.128/25'::inet);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.100.128/25'::inet);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.100.128/25'::inet);
SELECT topn(cms_topn_column, NULL::inet) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '192.168.100.128/23'::inet);
SELECT topn(cms_topn_column, NULL::inet) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, NULL::inet);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, NULL::inet);
SELECT topn(cms_topn_column, NULL::inet) from add_test;
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '10.1.2.3/32'::inet);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '10.1.2.3/32'::inet);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '10.1.2.3/32'::inet);
UPDATE add_test SET cms_topn_column = cms_topn_add(cms_topn_column, '10.1.2.3/32'::inet);
SELECT topn(cms_topn_column, NULL::inet) from add_test;


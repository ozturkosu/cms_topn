--
--Testing cms_topn function of the extension which creates new cms_topn structure
--

CREATE EXTENSION cms_topn;

--check errors for unproper parameters
SELECT cms_topn(0);
SELECT cms_topn(1, -0.1, 0.9);
SELECT cms_topn(-1, 0.1, 0.9);
SELECT cms_topn(3, 0.1, -0.5);
SELECT cms_topn(4, 0.02, 1.1);

--check proper parameters
CREATE TABLE create_test (
	cms_topn_column cms_topn
);

INSERT INTO create_test values(cms_topn(10));
INSERT INTO create_test values(cms_topn(5, 0.01, 0.95));
SELECT cms_topn_info(cms_topn_column) from create_test;

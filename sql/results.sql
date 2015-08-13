--
--Check functions of the extension which returns results like frequencies and the top n:
--topn, cms_topn_info, cms_topn_frequency.
--

--topn
--check with null and empty values
SELECT topn(NULL, NULL::integer);
SELECT topn(cms_topn(2), NULL::text);

--check with wrong parameters
SELECT topn(cms_topn_add(cms_topn(2),2),NULL::text);

--cms_topn_info
--check with null and empty values
SELECT cms_topn_info(NULL);
SELECT cms_topn_info(cms_topn(1));
SELECT cms_topn_info(cms_topn(2, 0.01, 0.99));

--check normal cases
SELECT cms_topn_info(cms_topn_add(cms_topn(1), 5));
SELECT cms_topn_info(cms_topn_add(cms_topn(2), 5));
SELECT cms_topn_info(cms_topn_add(cms_topn(2, 0.1, 0.9), '5'::text));

--cms_topn_frequency
--check with null and empty values
SELECT cms_topn_frequency(NULL, NULL::text);
SELECT cms_topn_frequency(NULL, 3::integer);
SELECT cms_topn_frequency(cms_topn_add(cms_topn(1),5), NULL::text);
SELECT cms_topn_frequency(cms_topn(3), NULL::bigint);

--check normal cases
CREATE TABLE results (
	cms_topn_column cms_topn
);

INSERT INTO results(cms_topn_column) SELECT cms_topn_add_agg(int_column, 3) FROM numbers;
SELECT cms_topn_frequency(cms_topn_column, 0) FROM results;
SELECT cms_topn_frequency(cms_topn_column, 1) FROM results;
SELECT cms_topn_frequency(cms_topn_column, 2) FROM results;
SELECT cms_topn_frequency(cms_topn_column, 3) FROM results;
SELECT cms_topn_frequency(cms_topn_column, 4) FROM results;
SELECT cms_topn_frequency(cms_topn_column, 5) FROM results;
SELECT cms_topn_frequency(cms_topn_column, -1) FROM results;
SELECT cms_topn_frequency(cms_topn_column, NULL::integer) FROM results;

DELETE FROM results;
INSERT INTO results(cms_topn_column) SELECT cms_topn_add_agg(text_column, 2) FROM strings;
SELECT cms_topn_frequency(cms_topn_column, '0'::text) FROM results;
SELECT cms_topn_frequency(cms_topn_column, '1'::text) FROM results;
SELECT cms_topn_frequency(cms_topn_column, '2'::text) FROM results;
SELECT cms_topn_frequency(cms_topn_column, '3'::text) FROM results;
SELECT cms_topn_frequency(cms_topn_column, '4'::text) FROM results;
SELECT cms_topn_frequency(cms_topn_column, '5'::text) FROM results;
SELECT cms_topn_frequency(cms_topn_column, '6'::text) FROM results;
SELECT cms_topn_frequency(cms_topn_column, NULL::text) FROM results;





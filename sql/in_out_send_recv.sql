--
--Testing cms_topn_in, cms_topn_out, cms_topn_send and cms_topn_recv functions
--

--in & out
COPY union_agg TO '/home/remzican/Documents/postgres/postgresql-9.4.0/contrib/cms_topn/results/in_out_test';
DELETE FROM union_agg;
COPY union_agg FROM '/home/remzican/Documents/postgres/postgresql-9.4.0/contrib/cms_topn/results/in_out_test';
SELECT topn(cms_topn_column, NULL::integer) FROM union_agg;

DELETE FROM union_agg;
INSERT INTO union_agg(cms_topn_column) SELECT cms_topn_add_agg(text_column, 3) FROM strings;

--send & recv
COPY union_agg TO '/home/remzican/Documents/postgres/postgresql-9.4.0/contrib/cms_topn/results/send_recv_test' WITH binary;
DELETE FROM union_agg;
COPY union_agg FROM '/home/remzican/Documents/postgres/postgresql-9.4.0/contrib/cms_topn/results/send_recv_test' WITH binary;
SELECT topn(cms_topn_column, NULL::text) FROM union_agg;

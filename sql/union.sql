--
--Testing cms_topn_union function of the extension
--

--check cases with null
SELECT cms_topn_union(NULL, NULL);
SELECT * FROM topn(cms_topn_union(cms_topn_add(cms_topn(1),4), NULL), NULL::integer);
SELECT * FROM topn(cms_topn_union(NULL, cms_topn_add(cms_topn(2),'cms_topn'::text)), NULL::text);

--check cases with empty cms_topn
SELECT * FROM topn(cms_topn_union(cms_topn(1), cms_topn(1)), NULL::integer);
SELECT * FROM topn(cms_topn_union(cms_topn(3), cms_topn_add(cms_topn(3),'cms_topn'::text)), NULL::text);
SELECT * FROM topn(cms_topn_union(cms_topn_add(cms_topn(2),4), cms_topn(2)), NULL::integer);

--check if parameters of the cms_topns are not the same
SELECT cms_topn_union(cms_topn(2), cms_topn(1));
SELECT cms_topn_union(cms_topn(1, 0.1, 0.9), cms_topn(1, 0.1, 0.8));
SELECT cms_topn_union(cms_topn(1, 0.1, 0.99), cms_topn(1, 0.01, 0.99));
SELECT cms_topn_union(cms_topn_add(cms_topn(2), 2), cms_topn_add(cms_topn(2), '2'::text));

--check normal cases
SELECT * FROM topn(cms_topn_union(cms_topn_add(cms_topn(1),2), cms_topn_add(cms_topn(1),3)), NULL::integer);
SELECT * FROM topn(cms_topn_union(cms_topn_add(cms_topn(1),2), cms_topn_add(cms_topn(1),2)), NULL::integer);
SELECT * FROM topn(cms_topn_union(cms_topn_add(cms_topn(2),'two'::text), cms_topn_add(cms_topn(2),'three'::text)), NULL::text);
SELECT * FROM topn(cms_topn_union(cms_topn_add(cms_topn(2),'two'::text), cms_topn_add(cms_topn(2),'two'::text)), NULL::text);
SELECT * FROM topn(cms_topn_union(cms_topn_add(cms_topn(3),'2'::text), cms_topn_add(cms_topn(3),'3'::text)), NULL::text);
SELECT * FROM topn(cms_topn_union(cms_topn_add(cms_topn(3),'2'::text), cms_topn_add(cms_topn(3),'2'::text)), NULL::text);

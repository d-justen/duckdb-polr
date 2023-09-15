SELECT COUNT(*)
FROM
tag as t,
site as s,
question as q,
tag_question as tq
WHERE
t.site_id = s.site_id
AND q.site_id = s.site_id
AND tq.site_id = s.site_id
AND tq.question_id = q.id
AND tq.tag_id = t.id
AND (s.site_name in ('math'))
AND (t.name in ('brownian-motion','complex-geometry','distribution-theory','examples-counterexamples','fractions','mathematical-physics','norm','normal-distribution','taylor-expansion'))
AND (q.favorite_count >= 0)
AND (q.favorite_count <= 10000)

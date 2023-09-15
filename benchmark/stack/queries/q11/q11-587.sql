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
AND (s.site_name in ('sharepoint'))
AND (t.name in ('2007','2016','client-object-model','content-type','csom','custom-list','designer-workflow','error','list-view','office-365','permissions','rest','sharepoint-addin'))
AND (q.score >= 0)
AND (q.score <= 1000)

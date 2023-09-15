SELECT acc.location, count(*)
FROM
site as s,
so_user as u1,
question as q1,
answer as a1,
tag as t1,
tag_question as tq1,
badge as b,
account as acc
WHERE
s.site_id = q1.site_id
AND s.site_id = u1.site_id
AND s.site_id = a1.site_id
AND s.site_id = t1.site_id
AND s.site_id = tq1.site_id
AND s.site_id = b.site_id
AND q1.id = tq1.question_id
AND q1.id = a1.question_id
AND a1.owner_user_id = u1.id
AND t1.id = tq1.tag_id
AND b.user_id = u1.id
AND acc.id = u1.account_id
AND (s.site_name in ('codereview','electronics','salesforce'))
AND (t1.name in ('apex','c++11','motor','object-oriented','pic','python-2.x'))
AND (q1.view_count >= 100)
AND (q1.view_count <= 100000)
AND (u1.reputation >= 10)
AND (u1.reputation <= 100000)
AND (b.name in ('Excavator','Fanatic','Good Answer','Good Question','Taxonomist'))
GROUP BY acc.location
ORDER BY COUNT(*)
DESC
LIMIT 100

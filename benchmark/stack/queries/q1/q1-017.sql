
select count(*) from tag, site, question, tag_question
where
site.site_name='superuser' and
tag.name='burn-in' and
tag.site_id = site.site_id and
question.site_id = site.site_id and
tag_question.site_id = site.site_id and
tag_question.question_id = question.id and
tag_question.tag_id = tag.id

SELECT COUNT(*) FROM title t,movie_info mi,movie_companies mc,cast_info ci,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mc.movie_id AND t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id=2 AND mi.info_type_id=16 AND t.production_year>2000 AND t.production_year<2010 AND mk.keyword_id=7084;


SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.production_year>1950 AND t.kind_id=1;


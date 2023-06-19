SELECT COUNT(*) FROM cast_info ci,title t,movie_companies mc WHERE t.id=ci.movie_id AND t.id=mc.movie_id AND t.production_year>2005 AND t.production_year<2015 AND ci.role_id=2;


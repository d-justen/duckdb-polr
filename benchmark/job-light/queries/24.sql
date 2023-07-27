SELECT COUNT(*) FROM cast_info ci,title t,movie_companies mc WHERE t.id=ci.movie_id AND t.id=mc.movie_id AND ci.role_id=4;


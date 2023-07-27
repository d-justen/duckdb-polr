SELECT COUNT(*) FROM movie_companies mc,title t,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=117;


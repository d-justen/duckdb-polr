SELECT COUNT(*) FROM title t,movie_keyword mk,movie_companies mc WHERE t.id=mk.movie_id AND t.id=mc.movie_id AND t.production_year>1950;


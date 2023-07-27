SELECT COUNT(*) FROM title t,movie_info mi,movie_companies mc,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.id=mc.movie_id AND mi.info_type_id=16 AND t.production_year>2000;


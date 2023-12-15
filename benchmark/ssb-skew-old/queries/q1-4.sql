SELECT min(c_name), min(p_name), min(c_nation), min(p_color)
FROM lineorder, locust, lopart, customer, part, date
WHERE lo_custkey = lc_locustkey
  AND lo_partkey = lp_lopartkey
  AND lo_orderdate = d_datekey
  AND lc_custkey = c_custkey
  AND lp_partkey = p_partkey
  AND c_nation = 'MOROCCO'
  AND p_type LIKE 'PROMO BURN%'
  AND d_sellingseason IN ('Winter', 'Spring');

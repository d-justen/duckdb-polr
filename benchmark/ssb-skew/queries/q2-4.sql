SELECT min(c_name), min(p_name), min(c_nation), min(p_color), min(s_region), min(s_nation)
FROM lineorder, locust, lopart, supplier, customer, part, date
WHERE lo_custkey = lc_locustkey
  AND lo_partkey = lp_lopartkey
  AND lc_custkey = c_custkey
  AND lp_partkey = p_partkey
  AND lo_suppkey = s_suppkey
  AND lo_orderdate = d_datekey
  AND s_region LIKE 'A%A'
  AND p_type LIKE '%BURN%'
  AND c_nation LIKE 'A%A'
  AND d_monthnuminyear BETWEEN 2 AND 8;

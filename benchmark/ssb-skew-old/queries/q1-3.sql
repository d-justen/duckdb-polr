SELECT min(c_name), min(p_name), min(c_nation), min(p_color)
FROM lineorder, locust, lopart, customer, part
WHERE lo_custkey = lc_locustkey
  AND lo_partkey = lp_lopartkey
  AND lc_custkey = c_custkey
  AND lp_partkey = p_partkey
  AND c_nation = 'MOROCCO'
  AND p_color = 'goldenrod';

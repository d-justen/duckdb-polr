SELECT min(lc_locustkey), min(lp_lopartkey), min(s_region), min(s_nation)
FROM lineorder, locust, lopart, supplier
WHERE lo_custkey = lc_locustkey
  AND lo_partkey = lp_lopartkey
  AND lo_suppkey = s_suppkey
  AND lc_custkey BETWEEN 1 AND 20
  AND lp_partkey BETWEEN 1 AND 20
  AND s_nation = 'PERU';

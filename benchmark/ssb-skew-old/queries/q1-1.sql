SELECT min(lc_custkey), min(lp_partkey)
FROM lineorder, locust, lopart
WHERE lo_custkey = lc_locustkey
  AND lo_partkey = lp_lopartkey
  AND lc_custkey = 1
  AND lp_partkey = 1;

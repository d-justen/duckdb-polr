SELECT min(lc_custkey), min(lp_partkey), min(d_date), min(d_dayofweek)
FROM lineorder, locust, lopart, date
WHERE lo_custkey = lc_locustkey
  AND lo_partkey = lp_lopartkey
  AND lo_orderdate = d_datekey
  AND d_month = 'July'
  AND lc_custkey BETWEEN 1 AND 50
  AND lp_partkey BETWEEN 1 AND 50;


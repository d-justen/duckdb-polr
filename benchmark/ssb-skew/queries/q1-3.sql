SELECT MIN(c_city), MIN(c_name), MIN(c_nation), MIN(c_region), MIN(d_date), MIN(d_dayofweek), MIN(d_month), MIN(d_sellingseason)
FROM lineorder, customer, date
WHERE lo_orderdate = d_datekey
  AND lo_custkey = c_custkey
  AND c_name IN ('Customer#000000002', 'Customer#000000006')
  AND d_sellingseason IN ('Summer', 'Fall');

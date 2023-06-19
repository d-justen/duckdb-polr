SELECT MIN(c_city), MIN(d_date)
FROM lineorder, customer, date
WHERE lo_orderdate = d_datekey
  AND lo_custkey = c_custkey
  AND c_nation = 'JORDAN'
  AND d_year BETWEEN 1994 AND 1998;

CREATE TABLE lineorder (
                           LO_ORDERKEY             UINTEGER NOT NULL,
                           LO_LINENUMBER           USMALLINT NOT NULL,
                           LO_CUSTKEY              UINTEGER NOT NULL,
                           LO_PARTKEY              UINTEGER NOT NULL,
                           LO_SUPPKEY              UINTEGER NOT NULL,
                           LO_ORDERDATE            UINTEGER NOT NULL,
                           LO_ORDERPRIORITY        VARCHAR,
                           LO_SHIPPRIORITY         USMALLINT,
                           LO_QUANTITY             USMALLINT,
                           LO_EXTENDEDPRICE        UINTEGER,
                           LO_ORDTOTALPRICE        UINTEGER,
                           LO_DISCOUNT             USMALLINT,
                           LO_REVENUE              UINTEGER,
                           LO_SUPPLYCOST           UINTEGER,
                           LO_TAX                  USMALLINT,
                           LO_COMMITDATE           UINTEGER NOT NULL,
                           LO_SHIPMODE             VARCHAR
);

CREATE TABLE customer (
                          C_CUSTKEY       UINTEGER NOT NULL PRIMARY KEY,
                          C_NAME	      VARCHAR,
                          C_ADDRESS       VARCHAR,
                          C_CITY          VARCHAR,
                          C_NATION        VARCHAR,
                          C_REGION        VARCHAR,
                          C_PHONE         VARCHAR,
                          C_MKTSEGMENT    VARCHAR
);

CREATE TABLE part (
                      P_PARTKEY       UINTEGER NOT NULL PRIMARY KEY,
                      P_NAME          VARCHAR,
                      P_MFGR          VARCHAR,
                      P_CATEGORY      VARCHAR,
                      P_BRAND         VARCHAR,
                      P_COLOR         VARCHAR,
                      P_TYPE          VARCHAR,
                      P_SIZE          USMALLINT,
                      P_CONTAINER     VARCHAR
);

CREATE TABLE supplier (
                          S_SUPPKEY    UINTEGER NOT NULL PRIMARY KEY,
                          S_NAME	VARCHAR,
                          S_ADDRESS VARCHAR,
                          S_CITY VARCHAR,
                          S_NATION VARCHAR,
                          S_REGION VARCHAR,
                          S_PHONE VARCHAR
);

CREATE TABLE date (
                      D_DATEKEY UINTEGER NOT NULL PRIMARY KEY,
                      D_DATE VARCHAR,
                      D_DAYOFWEEK VARCHAR,
                      D_MONTH VARCHAR,
                      D_YEAR USMALLINT,
                      D_YEARMONTHNUM UINTEGER,
                      D_YEARMONTH VARCHAR,
                      D_DAYNUMINWEEK USMALLINT,
                      D_DAYNUMINMONTH USMALLINT,
                      D_DAYNUMINYEAR USMALLINT,
                      D_MONTHNUMINYEAR USMALLINT,
                      D_WEEKNUMINYEAR USMALLINT,
                      D_SELLINGSEASON VARCHAR,
                      D_LASTDAYINWEEKFL BOOLEAN,
                      D_LASTDAYINMONTHFL BOOLEAN,
                      D_HOLIDAYFL BOOLEAN,
                      D_WEEKDAYFL BOOLEAN
);

COPY lineorder FROM 'PATHVAR/lineorder.tbl' (DELIMITER '|');
COPY customer FROM 'PATHVAR/customer.tbl' (DELIMITER '|');
COPY part FROM 'PATHVAR/part.tbl' (DELIMITER '|');
COPY supplier FROM 'PATHVAR/supplier.tbl' (DELIMITER '|');
COPY date FROM 'PATHVAR/date.tbl' (DELIMITER '|');

-- CUSTOMER
UPDATE customer SET c_region = 'AMERICA', c_nation = 'UNITED STATES', c_city = 'TODO' --TODO: city
WHERE c_region <> 'AMERICA' AND c_custkey % 10 <> 0;

CREATE TABLE additional_cust (ac_id INTEGER, ac_nation VARCHAR);
INSERT INTO additional_cust VALUES (0, 'ANDORRA'), (1, 'BAHAMAS'), (2, 'CAMBODIA'), (3, 'DENMARK'), (4, 'ECUADOR'),
                                   (5, 'FINLAND'), (6, 'GEORGIA'), (7, 'HONDURAS'), (8, 'ISRAEL'), (9, 'JAMAICA'),
                                   (10, 'KUWAIT'), (11, 'LAOS'), (12, 'LIECHTENSTEIN'), (13, 'MALAYSIA'),
                                   (14, 'NEPAL'), (15, 'OMAN'), (16, 'PANAMA'), (17, 'QATAR'), (18,'ROMANIA'),
                                   (19, 'SERBIA'), (20, 'THAILAND'), (21, 'UNITED ARAB EMIRATES'),
                                   (22, 'VENEZUELA'), (23, 'YEMEN'), (24, 'ZIMBABWE');

INSERT INTO customer SELECT row_number() OVER () + 3000000, c_name, c_address, CONCAT(ac_nation, ' 1'), ac_nation, 'OCEANIA', c_phone, c_mktsegment
FROM customer, additional_cust
WHERE c_custkey <= 100;

DROP TABLE additional_cust;

CREATE VIEW c1 AS SELECT row_number() OVER () - 1 AS c_id, c_custkey AS c_custkey1 FROM customer WHERE c_region <> 'ASIA';
CREATE VIEW c2 AS SELECT row_number() OVER () - 1 AS c_id, c_custkey AS c_custkey1 FROM customer WHERE c_region = 'ASIA';
CREATE VIEW c3 AS SELECT row_number() OVER () - 1 AS c_id, c_custkey AS c_custkey1 FROM customer WHERE c_city IN ('UNITED KI1', 'UNITED KI5');

UPDATE lineorder SET lo_custkey = (
    SELECT c_custkey1
    FROM c1
    WHERE lo_orderkey % 2942519 = c_id
    )
    FROM customer
WHERE lo_custkey = c_custkey
  AND c_region = 'ASIA'
  AND lo_orderkey < 400000000;

UPDATE lineorder SET lo_custkey = (
    SELECT c_custkey1
    FROM c2
    WHERE lo_orderkey % 59981 = c_id
    )
FROM customer
WHERE lo_custkey = c_custkey
AND c_region = 'AMERICA'
AND lo_orderkey % 3 == 0
AND lo_orderkey >= 400000000;

UPDATE lineorder SET lo_custkey = (
    SELECT c_custkey1
    FROM c3
    WHERE lo_orderkey % 2402 = c_id
FROM customer
WHERE lo_custkey = c_custkey
  AND c_region = 'AMERICA'
  AND lo_quantity >= 38
  AND lo_orderkey >= 400000000;

DROP VIEW c1;
DROP VIEW c2;
DROP VIEW c3;

-- SUPPLIER
UPDATE supplier SET s_region = 'ASIA', s_nation = 'CHINA', s_city = 'TODO' --TODO: city
WHERE s_region <> 'ASIA' AND s_suppkey % 10 <> 0;

CREATE VIEW s1 AS SELECT row_number() OVER () - 1 AS s_id, s_suppkey AS s_suppkey1 FROM supplier WHERE s_region = 'ASIA';
CREATE VIEW s2 AS SELECT row_number() OVER () - 1 AS s_id, s_suppkey AS s_suppkey1 FROM supplier WHERE s_nation = 'UNITED STATES';
CREATE VIEW s3 AS SELECT row_number() OVER () - 1 AS s_id, s_suppkey AS s_suppkey1 FROM supplier WHERE s_city IN ('UNITED KI1', 'UNITED KI5');

UPDATE lineorder SET lo_suppkey = (
    SELECT s_suppkey1
    FROM s1
    WHERE lo_orderkey % 183979 = s_id
    )
FROM supplier
WHERE lo_suppkey = s_suppkey
  AND s_nation = 'UNITED STATES'
  AND lo_orderkey < 400000000;

UPDATE lineorder SET lo_suppkey = (
    SELECT s_suppkey1
    FROM s2
    WHERE lo_orderkey % 787 = s_id
    )
FROM supplier
WHERE lo_suppkey = s_suppkey
  AND s_region = 'ASIA'
  AND lo_orderkey >= 400000000
  AND lo_quantity <= 6;

UPDATE lineorder SET lo_suppkey = (
    SELECT s_suppkey1
    FROM s3
    WHERE lo_orderkey % 191 = s_id
FROM supplier, customer
WHERE lo_suppkey = s_suppkey
  AND lo_custkey = c_custkey
  AND s_region = 'ASIA'
  AND c_city NOT IN ('UNITED KI1', 'UNITED KI5')
  AND lo_quantity >= 43
  AND lo_orderkey < 400000000;

DROP VIEW s1;
DROP VIEW s2;
DROP VIEW s3;

-- PART
UPDATE part SET p_category = 'MFGR#12' WHERE p_category <> 'MFGR#12' AND p_size <= 25;
UPDATE part SET p_category = 'MFGR#14' WHERE p_category <> 'MFGR#14' AND p_category <> 'MFGR#12' AND p_partkey % 2 = 0;
UPDATE part SET p_brand = 'MFGR#2239' WHERE p_partkey % 3 = 0;

-- DATE
CREATE VIEW d92 AS SELECT row_number() OVER () - 1 AS d_id, d_datekey FROM date WHERE D_YEAR = 1992;
CREATE VIEW d93 AS SELECT row_number() OVER () - 1 AS d_id, d_datekey FROM date WHERE D_YEAR = 1993;
CREATE VIEW d94 AS SELECT row_number() OVER () - 1 AS d_id, d_datekey FROM date WHERE D_YEAR = 1994;
CREATE VIEW d95 AS SELECT row_number() OVER () - 1 AS d_id, d_datekey FROM date WHERE D_YEAR = 1995;
CREATE VIEW d96 AS SELECT row_number() OVER () - 1 AS d_id, d_datekey FROM date WHERE D_YEAR = 1996;
CREATE VIEW d97 AS SELECT row_number() OVER () - 1 AS d_id, d_datekey FROM date WHERE D_YEAR = 1997;
CREATE VIEW d98 AS SELECT row_number() OVER () - 1 AS d_id, d_datekey FROM date WHERE D_YEAR = 1998;

UPDATE lineorder SET lo_orderdate = (
    SELECT d_datekey
    FROM d92
    WHERE lo_orderkey % 365 = d_id
    )
WHERE lo_orderkey > 0 AND lo_orderkey <= 80000000;

UPDATE lineorder SET lo_orderdate = (
    SELECT d_datekey
    FROM d93
    WHERE lo_orderkey % 365 = d_id
    )
WHERE lo_orderkey > 80000000 AND lo_orderkey <= 160000000;

UPDATE lineorder SET lo_orderdate = (
    SELECT d_datekey
    FROM d94
    WHERE lo_orderkey % 365 = d_id
    )
WHERE lo_orderkey > 160000000 AND lo_orderkey <= 240000000;

UPDATE lineorder SET lo_orderdate = (
    SELECT d_datekey
    FROM d95
    WHERE lo_orderkey % 365 = d_id
    )
WHERE lo_orderkey > 240000000 AND lo_orderkey <= 320000000;

UPDATE lineorder SET lo_orderdate = (
    SELECT d_datekey
    FROM d96
    WHERE lo_orderkey % 365 = d_id
    )
WHERE lo_orderkey > 320000000 AND lo_orderkey <= 400000000;

UPDATE lineorder SET lo_orderdate = (
    SELECT d_datekey
    FROM d97
    WHERE lo_orderkey % 365 = d_id
    )
WHERE lo_orderkey > 400000000 AND lo_orderkey <= 401000000;

UPDATE lineorder SET lo_orderdate = (
    SELECT d_datekey
    FROM d98
    WHERE lo_orderkey % 365 = d_id
    )
WHERE lo_orderkey > 401000000;

DROP VIEW d92;
DROP VIEW d93;
DROP VIEW d94;
DROP VIEW d95;
DROP VIEW d96;
DROP VIEW d97;
DROP VIEW d98;

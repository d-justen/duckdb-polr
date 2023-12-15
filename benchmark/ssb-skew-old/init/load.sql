CREATE TABLE lineorder_tmp (
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
                          C_NAME          VARCHAR,
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
                          S_SUPPKEY UINTEGER NOT NULL PRIMARY KEY,
                          S_NAME VARCHAR,
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

COPY lineorder_tmp FROM 'PATHVAR/lineorder.tbl.gz' (DELIMITER '|');
COPY customer FROM 'PATHVAR/customer.tbl.gz' (DELIMITER '|');
COPY part FROM 'PATHVAR/part.tbl.gz' (DELIMITER '|');
COPY supplier FROM 'PATHVAR/supplier.tbl.gz' (DELIMITER '|');
COPY date FROM 'PATHVAR/date.tbl.gz' (DELIMITER '|');

UPDATE lineorder_tmp SET lo_custkey = 1 FROM date WHERE d_datekey = lo_orderdate AND d_monthnuminyear BETWEEN 1 AND 3;
UPDATE lineorder_tmp SET lo_partkey = 1 FROM date WHERE d_datekey = lo_orderdate AND d_monthnuminyear BETWEEN 7 AND 9;
UPDATE lineorder_tmp SET lo_suppkey = 1 FROM date WHERE d_datekey = lo_orderdate and d_monthnuminyear IN (1, 2, 7, 8);

CREATE TABLE locust AS SELECT DISTINCT lo_custkey AS lc_locustkey, lo_custkey AS lc_custkey FROM lineorder_tmp;

INSERT INTO locust (
    SELECT 1, c_custkey
    FROM customer
    WHERE c_custkey BETWEEN 1 AND 20
);

CREATE TABLE lopart AS SELECT DISTINCT lo_partkey AS lp_lopartkey, lo_partkey AS lp_partkey from lineorder_tmp;

INSERT INTO lopart (
    SELECT 1, p_partkey
    FROM part
    WHERE p_partkey BETWEEN 1 AND 20
);

CREATE TABLE lineorder AS SELECT * FROM lineorder_tmp ORDER BY lo_orderdate;
DROP TABLE lineorder_tmp;
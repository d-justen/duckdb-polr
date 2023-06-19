CREATE TABLE lineorder_tmp (
    LO_ORDERKEY             UINTEGER,
    LO_LINENUMBER           USMALLINT,
    LO_CUSTKEY              UINTEGER,
    LO_PARTKEY              UINTEGER,
    LO_SUPPKEY              UINTEGER,
    LO_ORDERDATE            UINTEGER,
    LO_ORDERPRIORITY        VARCHAR,
    LO_SHIPPRIORITY         USMALLINT,
    LO_QUANTITY             USMALLINT,
    LO_EXTENDEDPRICE        UINTEGER,
    LO_ORDTOTALPRICE        UINTEGER,
    LO_DISCOUNT             USMALLINT,
    LO_REVENUE              UINTEGER,
    LO_SUPPLYCOST           UINTEGER,
    LO_TAX                  USMALLINT,
    LO_COMMITDATE           UINTEGER,
    LO_SHIPMODE             VARCHAR
);

CREATE TABLE customer (
    C_CUSTKEY       UINTEGER,
    C_NAME          VARCHAR,
    C_ADDRESS       VARCHAR,
    C_CITY          VARCHAR,
    C_NATION        VARCHAR,
    C_REGION        VARCHAR,
    C_PHONE         VARCHAR,
    C_MKTSEGMENT    VARCHAR
);

CREATE TABLE part (
                      P_PARTKEY       UINTEGER,
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
    S_SUPPKEY UINTEGER,
    S_NAME VARCHAR,
    S_ADDRESS VARCHAR,
    S_CITY VARCHAR,
    S_NATION VARCHAR,
    S_REGION VARCHAR,
    S_PHONE VARCHAR
);

CREATE TABLE date (
                      D_DATEKEY UINTEGER,
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

INSERT INTO lineorder_tmp SELECT * FROM read_parquet('../ssb-sf10/lineorder.parquet');
INSERT INTO customer SELECT * FROM read_parquet('../ssb-sf10/customer.parquet');
INSERT INTO part SELECT * FROM read_parquet('../ssb-sf10/part.parquet');
INSERT INTO supplier SELECT * FROM read_parquet('../ssb-sf10/supplier.parquet');
INSERT INTO date SELECT * FROM read_parquet('../ssb-sf10/date.parquet');

UPDATE lineorder_tmp SET lo_custkey = 2 FROM date WHERE lo_orderdate = d_datekey AND d_year >= 1992 AND d_year <= 1994 AND d_monthnuminyear >= 1 AND d_monthnuminyear <= 4;
UPDATE lineorder_tmp SET lo_custkey = 6 FROM date WHERE lo_orderdate = d_datekey AND d_year >= 1995 AND d_year <= 1997 AND d_monthnuminyear >= 1 AND d_monthnuminyear <= 4;
UPDATE lineorder_tmp SET lo_suppkey = 1 FROM date WHERE lo_orderdate = d_datekey AND d_year >= 1995 AND d_year <= 1996;
CREATE TABLE lineorder AS SELECT * FROM lineorder_tmp ORDER BY lo_orderdate;
DROP TABLE lineorder_tmp;

CREATE COLUMN STORE ACCESS METHOD vertical HANDLER vertical_cstore_handler;

CREATE TABLE supplier (
        s_suppkey  INTEGER NOT NULL,
        s_name CHAR(25) NOT NULL,
        s_address VARCHAR(40) NOT NULL,
        s_nationkey INTEGER NOT NULL,
        s_phone CHAR(15) NOT NULL,
        s_acctbal FIXEDDECIMAL NOT NULL,
        s_comment VARCHAR(101) NOT NULL
);

CREATE TABLE part (
        p_partkey INTEGER NOT NULL,
        p_name VARCHAR(55) NOT NULL,
        p_mfgr CHAR(25) NOT NULL,
        p_brand CHAR(10) NOT NULL,
        p_type VARCHAR(25) NOT NULL,
        p_size FIXEDDECIMAL NOT NULL,
        p_container CHAR(10) NOT NULL,
        p_retailprice FIXEDDECIMAL NOT NULL,
        p_comment VARCHAR(23) NOT NULL,
        COLUMN STORE container USING vertical (p_container)
);

CREATE TABLE partsupp (
        ps_partkey INTEGER NOT NULL,
        ps_suppkey INTEGER NOT NULL,
        ps_availqty FIXEDDECIMAL NOT NULL,
        ps_supplycost FIXEDDECIMAL NOT NULL,
        ps_comment VARCHAR(199) NOT NULL,
        COLUMN STORE comment USING vertical (ps_comment)
);

CREATE TABLE customer (
        c_custkey INTEGER NOT NULL,
        c_name VARCHAR(25) NOT NULL,
        c_address VARCHAR(40) NOT NULL,
        c_nationkey INTEGER NOT NULL,
        c_phone CHAR(15) NOT NULL,
        c_acctbal FIXEDDECIMAL NOT NULL,
        c_mktsegment CHAR(10) NOT NULL,
        c_comment VARCHAR(117) NOT NULL,
       COLUMN STORE mktsegment USING vertical (c_mktsegment)
);


CREATE TABLE orders (
        o_orderkey BIGINT NOT NULL,
        o_custkey INTEGER NOT NULL,
        o_orderstatus CHAR(1) NOT NULL,
        o_totalprice FIXEDDECIMAL NOT NULL,
        o_orderdate DATE NOT NULL,
        o_orderpriority CHAR(15) NOT NULL,
        o_clerk CHAR(15) NOT NULL,
        o_shippriority INTEGER NOT NULL,
        o_comment VARCHAR(79) NOT NULL,
        COLUMN STORE clerk USING vertical (o_clerk),
        COLUMN STORE orderstatus USING vertical (o_orderstatus),
        COLUMN STORE comment USING vertical (o_comment)
);


CREATE TABLE lineitem (
        l_orderkey BIGINT NOT NULL,
        l_partkey INTEGER NOT NULL,
        l_suppkey INTEGER NOT NULL,
        l_linenumber INTEGER NOT NULL,
        l_quantity FIXEDDECIMAL NOT NULL,
        l_extendedprice FIXEDDECIMAL NOT NULL,
        l_discount FIXEDDECIMAL NOT NULL,
        l_tax FIXEDDECIMAL NOT NULL,
        l_returnflag CHAR(1) NOT NULL,
        l_linestatus CHAR(1) NOT NULL,
        l_shipdate DATE NOT NULL,
        l_commitdate DATE NOT NULL,
        l_receiptdate DATE NOT NULL,
        l_shipinstruct CHAR(25) NOT NULL,
        l_shipmode CHAR(10) NOT NULL,
        l_comment VARCHAR(44) NOT NULL,
        COLUMN STORE comment USING vertical (l_comment)
);


CREATE TABLE nation (
        n_nationkey INTEGER NOT NULL,
        n_name CHAR(25) NOT NULL,
        n_regionkey INTEGER NOT NULL,
        n_comment VARCHAR(152) NOT NULL
);

CREATE TABLE region (
        r_regionkey INTEGER NOT NULL,
        r_name CHAR(25) NOT NULL,
        r_comment VARCHAR(152) NOT NULL
);


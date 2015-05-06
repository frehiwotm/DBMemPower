select * from partsupp where ps_supplycost < 500 ;
select p_partkey, p_name, p_retailprice from part where p_retailprice < 100 ;
select sum (s_acctbal + (s_acctbal * .05)) from supplier ;
select sum (ps_supplycost) from supplier, partsupp where s_suppkey = ps_suppkey ;
select distinct ps_suppkey from partsupp where ps_supplycost < 100.11 ;
select sum (ps_supplycost) from supplier, partsupp where s_suppkey = ps_suppkey group by s_nationkey ;
select sum(ps_supplycost) from part, supplier, partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_acctbal > 2500 ;
select l_orderkey, l_partkey, l_suppkey from lineitem where l_returnflag = 'R' and l_discount < 0.04 or l_returnflag = 'R' and l_shipmode = 'MAIL' ;

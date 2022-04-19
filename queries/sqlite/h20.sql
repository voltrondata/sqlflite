SELECT s_name,
       s_address
FROM supplier,
     nation
WHERE s_suppkey in
    (SELECT ps_suppkey
     FROM partsupp
     WHERE ps_partkey in
         (SELECT p_partkey
          FROM part
          WHERE p_name like 'forest%' )
       AND ps_availqty >
         (SELECT 0.5 * sum(l_quantity)
          FROM lineitem
          WHERE l_partkey = ps_partkey
            AND l_suppkey = ps_suppkey
            AND l_shipdate >= '1994-01-01'
            AND l_shipdate < '1995-01-01' ) )
  AND s_nationkey = n_nationkey
  AND n_name = 'CANADA'
ORDER BY s_name
;

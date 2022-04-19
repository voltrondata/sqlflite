SELECT p_brand,
       p_type,
       p_size,
       count(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp,
     part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type not like 'MEDIUM POLISHED%'
  AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey not in
    (SELECT s_suppkey
     FROM supplier
     WHERE s_comment like '%Customer%Complaints%' )
GROUP BY p_brand,
         p_type,
         p_size
ORDER BY supplier_cnt DESC,
         p_brand,
         p_type,
         p_size
;

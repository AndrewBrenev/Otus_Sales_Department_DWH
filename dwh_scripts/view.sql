CREATE VIEW mv_bi_companies_statistic AS
SELECT company_id, count(*) as signs_count FROM companies_activity ca WHERE event_type = 'SIGN' group by company_id;


CREATE VIEW mv_bi_statistics as
SELECT * FROM sales_statistics.sellers s
  RIGHT JOIN sales_statistics.sellers_activity sa on s.id = sa.seller_id
  RIGHT JOIN sales_statistics.tariffs t ON sa.tariff_id = t.id
 RIGHT JOIN sales_statistics.mv_bi_companies_statistic mv ON mv.company_id = sa.company_id;
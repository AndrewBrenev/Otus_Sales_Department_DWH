CREATE VIEW sales_statistics.mv_bi_companies_statistic AS
SELECT company_id, count(*) as signs_count FROM companies_activity ca WHERE event_type = 'SIGN' group by company_id;


CREATE VIEW sales_statistics.mv_bi_statistics as
SELECT *
FROM sales_statistics.sellers_activity deal
  RIGHT JOIN sales_statistics.sellers seller on seller.id = deal.seller_id
  RIGHT JOIN sales_statistics.company company  on deal.company_id = company.id 
  RIGHT JOIN sales_statistics.tariffs tariff ON deal.tariff_id = tariff.id
  RIGHT JOIN sales_statistics.mv_bi_companies_statistic mv ON mv.company_id = deal.company_id;
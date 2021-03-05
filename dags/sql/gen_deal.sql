
drop table if exists deals;

Create table deals as (
Select origin_airport_cd,destination_airport_cd, quote_price, average_price,CURRENT_DATE as deal_date, ROUND((average_price-quote_price)/(average_price),2) as deal_value, min_price, max_price   from (
Select date_of_api_pull_key as date_deal, origin_airport_cd, destination_airport_cd, quote_price, 
ROUND(AVG(quote_price) OVER (PARTITION BY origin_airport_cd, destination_airport_cd),2) as average_price,
MIN(quote_price) OVER (PARTITION  BY origin_airport_cd, destination_airport_cd) as min_price,
MAX(quote_price) OVER (PARTITION  BY origin_airport_cd, destination_airport_cd) as max_price,
ROW_NUMBER() OVER (PARTITION BY origin_airport_cd, destination_airport_cd ORDER BY date_of_api_pull_key desc) as rownum
from quote_fact 
where date_of_api_pull_key < '{{ params.date_of_api_pull }}'
   ) a
 where rownum=1
 group by origin_airport_cd,destination_airport_cd,quote_price, average_price, date_deal, min_price, max_price
 );
CREATE TABLE summary AS
SELECT 
      year AS publication_year,
        COUNT(*) AS book_count,
        ROUND(AVG(CASE 
                 WHEN price LIKE '€%' THEN CAST(REPLACE(price, '€', '') AS NUMERIC) * 1.2
                    ELSE CAST(REPLACE(price, '$', '') AS NUMERIC)
                    END), 2) AS average_price_usd
FROM books
GROUP BY year  
ORDER BY publication_year;
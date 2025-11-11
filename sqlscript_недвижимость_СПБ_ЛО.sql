-- Время активности объявлений
WITH limits AS ( -- CTE для ограничения выбросов
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     ),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)    ),
-- Выведем объявления без выбросов:
corrected_flats AS (SELECT *
FROM real_estate.flats f
INNER JOIN real_estate.advertisement a USING(id)
INNER JOIN real_estate.city c USING(city_id)
INNER JOIN real_estate."type" t USING(type_id)
WHERE id IN (SELECT * FROM filtered_id)),
groupped_flats AS (SELECT *,
CASE 
	WHEN cf.city = 'Санкт-Петербург' THEN 'Санкт-Петербург'
	ELSE 'Лен.обл.'
END AS region, -- разделили по региону
CASE 
	WHEN cf.days_exposition < 30 THEN 'до месяца'
	WHEN cf.days_exposition >=30 AND cf.days_exposition < 90 THEN 'до трех месяцев'
	WHEN cf.days_exposition >=90 AND cf.days_exposition < 180 THEN 'до полугода'
	WHEN cf.days_exposition >=180 THEN 'больше полугода'
	WHEN cf.days_exposition IS NULL THEN 'не закрыты'
END AS segment, -- разделили на сегменты
cf.last_price::numeric/cf.total_area AS metre_price
FROM corrected_flats cf 
WHERE cf."type" = 'город') -- отфильтровали только города
SELECT region, segment, COUNT(gp.id), round(COUNT(*)/SUM(COUNT(*)) OVER()::NUMERIC, 2) AS share_total,
round(COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY region)::NUMERIC, 2) AS share_region,       
round(AVG(gp.metre_price)::numeric,2) AS avg_metre_price, 
round(AVG(gp.total_area)::numeric, 2) AS avg_area, round(AVG(gp.rooms)::numeric, 2) AS avg_rooms, 
round(AVG(gp.balcony)::numeric, 2) AS avg_balcony, round(AVG(gp.floor)::numeric, 2) AS avg_floor,
round(AVG(days_exposition)::NUMERIC, 2) AS avg_exp_days -- рассчитали средние значения
FROM groupped_flats gp
WHERE metre_price<1907500 -- исключили выброс
GROUP BY region, segment; -- задача 1

--Сезонность объявлений
WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     ),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)    ),
-- Выведем объявления без выбросов:
corrected_flats AS (SELECT *, 
(DATE_TRUNC('month', a.first_day_exposition)::date + make_interval(days => a.days_exposition::int))::date AS last_month
FROM real_estate.flats f
JOIN real_estate.advertisement a USING(id)
JOIN real_estate.city c USING(city_id)
JOIN real_estate."type" t USING(type_id) -- объединяем с остальными таблицами
WHERE EXTRACT(YEAR from a.first_day_exposition) NOT IN (2014, 2019)), -- убираем годы с неполными данными
publicated AS (SELECT to_char(date_trunc('month', first_day_exposition)::date, 'Month') AS exp_month, -- выделяем месяц публикации
rank() over(ORDER BY count(id) desc),
count(id) AS total_count, round(avg(last_price::NUMERIC/total_area)::NUMERIC, 2) AS avg_price_per_metre, round(AVG(total_area)::numeric, 2) AS avg_area
FROM corrected_flats
WHERE "type" = 'город' AND last_price::NUMERIC/total_area<1907500 -- исключаем выброс и оставляем только города
GROUP BY to_char(date_trunc('month', first_day_exposition)::date, 'Month')  ), -- опубликованные объявления
selled AS (SELECT to_char(date_trunc('month', last_month)::date, 'Month') AS last_exp_month,  -- выделяем месяц
rank() over(ORDER BY count(id) desc),
count(id) AS total_count, round(avg(last_price::NUMERIC/total_area)::NUMERIC, 2) AS avg_price_per_metre, round(AVG(total_area)::numeric, 2) AS avg_area
FROM corrected_flats
WHERE days_exposition IS NOT NULL AND last_price::NUMERIC/total_area<1907500 AND "type" = 'город'-- исключаем выброс
GROUP BY to_char(date_trunc('month', last_month)::date, 'Month')  ) -- снятые с публикации
SELECT *
FROM publicated p
FULL JOIN selled s ON p.exp_month=last_exp_month
ORDER BY p.total_count desc
; -- задача 2

-- Анализ рынка недвижимости Ленобласти
WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     ),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)    ),
-- Выведем объявления без выбросов:
corrected_flats AS (SELECT *
FROM real_estate.flats f
JOIN real_estate.advertisement a USING(id)
JOIN real_estate.city c USING(city_id)
JOIN real_estate."type" t USING(type_id)), -- объединяем с остальными таблицами
main_cte AS (SELECT city, count(id) AS total_count, -- считаем общее кол-во объявлений
count(days_exposition) AS selled_count, -- считаем кол-во снятых с публикации объявлений
round(AVG(last_price::numeric/total_area)::numeric, 2)  AS metre_price, 
round(AVG(total_area)::NUMERIC, 2) AS avg_area, 
round(AVG(days_exposition)::NUMERIC, 2) AS avg_exp_days -- считаем средние показатели
FROM corrected_flats cf
WHERE city != 'Санкт-Петербург'-- оставляем только ЛО
GROUP BY city
ORDER BY count(id) DESC
LIMIT 15)
SELECT city, selled_count, total_count, 
round((selled_count::NUMERIC/total_count)*100, 2) AS percentage, -- считаем долю проданных квартир
metre_price, avg_area, avg_exp_days
FROM main_cte; -- задача 3


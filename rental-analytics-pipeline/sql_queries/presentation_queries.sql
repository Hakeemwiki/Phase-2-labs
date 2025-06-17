
-- Occupancy Rate by Monthly
SELECT 
    TO_CHAR(DATE_TRUNC('month', b.checkin_date::DATE), 'YYYY-MM') AS month,
    SUM(DATEDIFF(day, b.checkin_date::DATE, b.checkout_date::DATE)) AS nights_booked,
    COUNT(DISTINCT b.apartment_id) * 30 AS total_available_nights,
    ROUND(SUM(DATEDIFF(day, b.checkin_date::DATE, b.checkout_date::DATE))::DECIMAL 
          / (COUNT(DISTINCT b.apartment_id) * 30), 2) AS occupancy_rate
FROM curated.bookings b
WHERE b.booking_status = 'confirmed'
GROUP BY 1
ORDER BY 1;

-- average_listing_price_weekly
SELECT 
    TO_CHAR(DATE_TRUNC('week', a.listing_created_on), 'YYYY-MM-DD') AS week,
    ROUND(AVG(a.price), 2) AS avg_listing_price
FROM curated.apartments a
WHERE a.is_active = TRUE
GROUP BY 1
ORDER BY 1;

-- top_performing_listings_weekly
SELECT 
    TO_CHAR(DATE_TRUNC('week', booking_date::DATE), 'YYYY-MM-DD') AS week,
    apartment_id,
    SUM(total_price) AS total_revenue
FROM curated.bookings
WHERE booking_status = 'confirmed'
GROUP BY 1, 2
ORDER BY 1, 3 DESC

-- most_popular_locations_weekly
-- joining bookings with apartments to get city names
SELECT 
  DATE_FORMAT(DATE_TRUNC('WEEK', booking_date), 'yyyy-MM-dd') AS week,
  cityname,
  COUNT(*) AS total_bookings
FROM myDataSource
WHERE booking_status = 'confirmed'
GROUP BY 
  DATE_FORMAT(DATE_TRUNC('WEEK', booking_date), 'yyyy-MM-dd'),
  cityname
ORDER BY 
  week, 
  total_bookings DESC


-- total_bookings_per_user_weekly
SELECT
    TO_CHAR(DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')), 'YYYY-MM-DD') AS week,
    COUNT(*) AS total_bookings
FROM curated.bookings
WHERE booking_status = 'confirmed'
GROUP BY 1
ORDER BY 1;

-- repeat_customers_rate_monthly
WITH user_bookings AS (
    SELECT 
        user_id, 
        booking_date::DATE AS booking_date,
        LEAD(booking_date::DATE) OVER (PARTITION BY user_id ORDER BY booking_date::DATE) AS next_booking_date
    FROM curated.bookings
    WHERE booking_status = 'confirmed'
),
repeat_customers AS (
    SELECT DISTINCT user_id
    FROM user_bookings
    WHERE next_booking_date IS NOT NULL 
      AND DATEDIFF(day, booking_date, next_booking_date) <= 30
)

SELECT 
    TO_CHAR(DATE_TRUNC('month', b.booking_date::DATE), 'YYYY-MM') AS month,
    COUNT(DISTINCT repeat_customers.user_id) AS repeat_customers
FROM curated.bookings b
JOIN repeat_customers ON b.user_id = repeat_customers.user_id
GROUP BY 1
ORDER BY 1


-- average_booking_duration_weekly
SELECT 
    TO_CHAR(DATE_TRUNC('week', booking_date::DATE), 'YYYY-MM-DD') AS week,
    ROUND(AVG(DATEDIFF(day, checkin_date::DATE, checkout_date::DATE)), 2) AS avg_booking_duration
FROM curated.bookings
WHERE booking_status = 'confirmed'
GROUP BY 1
ORDER BY 1

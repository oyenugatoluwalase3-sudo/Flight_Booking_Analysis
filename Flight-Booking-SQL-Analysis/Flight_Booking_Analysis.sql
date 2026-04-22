USE FlightBookingDB;
GO

-- ====================================================================
-- PHASE 1: DATABASE SETUP & TABLE METRICS
-- ====================================================================

-- Proof of Schema Creation (Representative structures based on CSV imports)
/*
CREATE TABLE flight_data.flight_clean (
    fc_id INT IDENTITY(1,1) PRIMARY KEY,
    airline VARCHAR(50),
    flight VARCHAR(20),
    source_city VARCHAR(50),
    departure_time VARCHAR(50),
    stops VARCHAR(20),
    arrive_time VARCHAR(50),
    destination_city VARCHAR(50),
    class VARCHAR(20),
    durations FLOAT,
    days_left INT,
    price INT
);
*/

-- Total Row count of each table
SELECT 'Flight_clean' AS Dataset, COUNT(*) AS TotalRows FROM flight_data.flight_clean
UNION ALL
SELECT 'Flight_buisness', COUNT(*) FROM flight_data.flight_buisness
UNION ALL
SELECT 'Flight_economy', COUNT(*) FROM flight_data.flight_economy;
GO

/*
-- PHASE 1 TABLE DESCRIPTIONS --
- flight_clean: The primary merged dataset containing all combined flight booking records.
- flight_economy: A raw dataset containing records specifically for Economy class tickets.
- flight_buisness: A raw dataset containing records specifically for Business class tickets.
*/

-- ====================================================================
-- PHASE 2: DATA UNDERSTANDING AND VALIDATION
-- ====================================================================

-- Checking for NULL values across all columns in the merged clean dataset
SELECT
	COUNT(*) AS TotalRows,
	SUM(CASE WHEN fc_id IS NULL THEN 1 ELSE 0 END) AS fc_id_nulls,
	SUM(CASE WHEN airline IS NULL THEN 1 ELSE 0 END) AS airline_nulls,
	SUM(CASE WHEN flight IS NULL THEN 1 ELSE 0 END) AS flight_nulls,
	SUM(CASE WHEN source_city IS NULL THEN 1 ELSE 0 END) AS source_city_nulls,
	SUM(CASE WHEN departure_time IS NULL THEN 1 ELSE 0 END) AS departure_time_nulls,
	SUM(CASE WHEN stops IS NULL THEN 1 ELSE 0 END) AS stops_nulls,
	SUM(CASE WHEN arrive_time IS NULL THEN 1 ELSE 0 END) AS arrive_time_nulls,
	SUM(CASE WHEN destination_city IS NULL THEN 1 ELSE 0 END) AS destination_city_nulls,
	SUM(CASE WHEN class IS NULL THEN 1 ELSE 0 END) AS class_nulls,
	SUM(CASE WHEN durations IS NULL  THEN 1 ELSE 0 END) AS durations_nulls,
	SUM(CASE WHEN days_left IS NULL THEN 1 ELSE 0 END) AS days_left_nulls,
	SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS price_nulls
FROM flight_data.flight_clean;
GO

-- Check for duplicate rows in `flight_clean` table
WITH DuplicateCheck AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY airline, flight, source_city, departure_time, stops, 
                         arrive_time, destination_city, class, durations, days_left, price
            ORDER BY fc_id 
        ) AS row_num
    FROM flight_data.flight_clean
)
SELECT COUNT(*) TotalDuplicate_fc FROM DuplicateCheck WHERE row_num > 1;
GO

/*
-- PHASE 2 DATA QUALITY REPORT --
1. Missing Values: 0 NULL values found across tables. The data is fully populated.
2. Duplicate Records: No duplicates found in the merged `flight_clean` dataset. 15,235 duplicates were isolated in the raw CSV tables, expected due to daily web-scraping behavior capturing identical daily prices.
*/

-- ====================================================================
-- PHASE 3: CORE FLIGHT ANALYSIS TASKS
-- ====================================================================

-- 1. Flight count by airline
SELECT airline, COUNT(*) AS total_flight_count
FROM flight_data.flight_clean
GROUP BY airline
ORDER BY total_flight_count DESC;
GO

-- 2. Average price by airline (Class Comparison)
SELECT
	airline,
	AVG(CASE WHEN class = 'Economy' THEN CAST(price AS BIGINT) END) AS avg_economy_price,
	AVG(CASE WHEN class = 'Business' THEN CAST(price AS BIGINT) END) AS avg_business_price
FROM flight_data.flight_clean
GROUP BY airline
ORDER BY avg_business_price DESC; 
GO

-- 3. Flight count by class (economy vs business)
SELECT class, COUNT(*) AS total_flight
FROM flight_data.flight_clean
GROUP BY class
ORDER BY total_flight DESC;
GO

-- 4. Flight distribution by number of stops
SELECT stops, COUNT(*) AS total_flight
FROM flight_data.flight_clean
GROUP BY stops
ORDER BY total_flight DESC;
GO

/*
-- PHASE 3 INSIGHTS --
- Vistara operates the highest volume of flights (127,859) while SpiceJet operates the least (9,011).
- Vistara commands the highest average prices across both Economy and Business classes.
- Economy class dominates the market volume (206,666 flights) compared to Business (93,487).
- The vast majority of flights (250,863) operate with exactly one stop.
*/

-- ====================================================================
-- PHASE 4: INTERMEDIATE PRICING AND ROUTE ANALYSIS
-- ====================================================================

-- 1. Airlines with highest average prices
SELECT airline, AVG(CAST(price AS BIGINT)) AS avg_price_airline
FROM flight_data.flight_clean
GROUP BY airline
ORDER BY avg_price_airline DESC;
GO

-- 2. Most expensive routes on average
SELECT source_city, destination_city, AVG(CAST(price AS BIGINT)) AS avg_price_routes
FROM flight_data.flight_clean
GROUP BY source_city, destination_city
ORDER BY avg_price_routes DESC;
GO

-- 3. Price comparison between classes for the same routes
SELECT
	source_city,
	destination_city,
	AVG(CASE WHEN class = 'Economy' THEN CAST(price AS BIGINT) END) AS avg_price_economy,
	AVG(CASE WHEN class = 'Business' THEN CAST(price AS BIGINT) END) AS avg_price_business,
	AVG(CASE WHEN class = 'Business' THEN CAST(price AS BIGINT) END) - 
    AVG(CASE WHEN class = 'Economy' THEN CAST(price AS BIGINT) END) AS price_differences
FROM flight_data.flight_clean
GROUP BY source_city, destination_city
ORDER BY avg_price_business DESC;
GO

-- 4. Variation in prices by departure time category
SELECT departure_time, AVG(CAST(price AS BIGINT)) AS avg_price_dept_time
FROM flight_data.flight_clean
GROUP BY departure_time
ORDER BY avg_price_dept_time DESC;
GO

-- ====================================================================
-- PHASE 5: MULTI-TABLE ANALYTICAL QUERIES
-- ====================================================================

-- 1. Join flight_clean with business logic
SELECT TOP 5 c.fc_id, c.airline, b.price AS raw_business_price
FROM flight_data.flight_clean AS c
INNER JOIN flight_data.flight_buisness AS b ON c.fc_id = b.fb_id;
GO

-- 2. Compare pricing patterns across business and economy (CTE & Data Scrubbing)
WITH Economy_Summary AS (
	SELECT airline, AVG(CAST(REPLACE(REPLACE(price, ',', ''), '"', '') AS BIGINT)) AS avg_economy_price
	FROM flight_data.flight_economy
	GROUP BY airline
),
Business_Summary AS (
	SELECT airline, AVG(CAST(REPLACE(REPLACE(price, ',', ''), '"', '') AS BIGINT)) AS avg_buisness_price
	FROM flight_data.flight_buisness
	GROUP BY airline
)
SELECT
	e.airline,
	e.avg_economy_price,
	b.avg_buisness_price AS avg_business_price,
	(b.avg_buisness_price - e.avg_economy_price) AS premium_difference
FROM Economy_Summary AS e
INNER JOIN Business_Summary AS b ON e.airline = b.airline;
GO

-- 3. Analyze route counts with class segmentation
SELECT
	source_city,
	destination_city,
	COUNT(CASE WHEN class = 'Economy' THEN 1 END) AS total_economy_flight_count,
	COUNT(CASE WHEN class = 'Business' THEN 1 END) AS total_business_flight_count
FROM flight_data.flight_clean
GROUP BY source_city, destination_city
ORDER BY total_business_flight_count DESC;
GO

/*
-- PHASE 5 JOIN LOGIC & INSIGHTS --
- Inner Join Logic: An INNER JOIN was utilized to compare class pricing to strictly calculate the premium difference for airlines operating BOTH Economy and Business classes, naturally filtering out single-class budget airlines.
- Pricing Patterns: Vistara commands a massive premium, charging roughly 47,671 more for a Business class ticket over Economy.
- Route Segmentation: The Delhi-Mumbai corridor is the undisputed king of premium travel, boasting the highest volume of Business class flights (>5,000 each way).
*/

-- ====================================================================
-- PHASE 6: WINDOW FUNCTION APPLICATION
-- ====================================================================

-- 1. Rank airlines by average ticket price
SELECT
	airline,
	AVG(CAST(price AS BIGINT)) AS avg_price,
	RANK() OVER (ORDER BY AVG(CAST(price AS BIGINT)) DESC) AS price_rank
FROM flight_data.flight_clean
GROUP BY airline;
GO

-- 2. Rank routes by total flight count
SELECT
    source_city,
    destination_city,
	COUNT(*) AS total_flights,
	RANK() OVER (ORDER BY COUNT(*) DESC) AS flight_count_rank
FROM flight_data.flight_clean
GROUP BY source_city, destination_city;
GO

-- 3. Compare individual flight prices to airline averages
SELECT TOP 100
	airline,
	source_city,
	destination_city,
	price AS actual_ticket_price,
	AVG(price) OVER (PARTITION BY airline) AS airline_avg_price,
	(price - AVG(price) OVER (PARTITION BY airline)) AS price_difference
FROM flight_data.flight_clean
ORDER BY price_difference DESC;
GO

/*
-- PHASE 6 WINDOW FUNCTION LOGIC --
- ORDER BY: Instructs the window function how to hand out ranks based on aggregated math.
- PARTITION BY: Acts as an isolated calculator, determining the average specifically for an airline and attaching it to row-level data without collapsing the dataset.
*/

-- ====================================================================
-- PHASE 7: TIME AND CONDITIONAL LOGIC
-- ====================================================================

-- 1. Categorize flights by departure time of day
SELECT 
	airline,
	COUNT(CASE WHEN departure_time = 'Early_Morning' THEN 1 END) AS early_morning_flights,
	COUNT(CASE WHEN departure_time = 'Morning' THEN 1 END) AS morning_flights,
	COUNT(CASE WHEN departure_time = 'Afternoon' THEN 1 END) AS afternoon_flights,
	COUNT(CASE WHEN departure_time = 'Evening' THEN 1 END) AS evening_flights,
	COUNT(CASE WHEN departure_time = 'Night' THEN 1 END) AS night_flights,
	COUNT(CASE WHEN departure_time = 'Late_Night' THEN 1 END) AS late_night_flights
FROM flight_data.flight_clean
GROUP BY airline
ORDER BY morning_flights DESC;
GO

-- 2. Create price bands (low, medium, high) based on quantiles
WITH Price_Quantiles AS (
	SELECT 
		airline,
		NTILE(3) OVER (ORDER BY price) AS price_bucket
	FROM flight_data.flight_clean
)
SELECT
	airline,
	COUNT(CASE WHEN price_bucket = 1 THEN 1 END) AS low_price_band,
	COUNT(CASE WHEN price_bucket = 2 THEN 1 END) AS medium_price_band,
	COUNT(CASE WHEN price_bucket = 3 THEN 1 END) AS high_price_band
FROM Price_Quantiles
GROUP BY airline;
GO

-- 3. Tag routes with high vs low price variability
SELECT
	source_city,
	destination_city,
	MAX(price) - MIN(price) AS price_range,
	CASE 
		WHEN (MAX(price) - MIN(price)) > 50000 THEN 'High Variability' 
		ELSE 'Low Variability' 
	END AS variability_tag
FROM flight_data.flight_clean
GROUP BY source_city, destination_city
ORDER BY price_range DESC;
GO

-- ====================================================================
-- PHASE 8: BUSINESS-FOCUSED REPORTING
-- ====================================================================

-- 1. Top 20 most expensive individual flights
SELECT TOP 20 airline, source_city, destination_city, class, price AS actual_ticket_price
FROM flight_data.flight_clean
ORDER BY price DESC;
GO

-- 2. Cheapest airlines for economy class
SELECT TOP 1 airline, AVG(price) AS avg_economy_price
FROM flight_data.flight_clean
WHERE class = 'Economy'
GROUP BY airline
ORDER BY avg_economy_price ASC;
GO

-- 3. Routes with highest price variability
SELECT TOP 5 source_city, destination_city, (MAX(price) - MIN(price)) AS price_range_variability
FROM flight_data.flight_clean
GROUP BY source_city, destination_city
ORDER BY price_range_variability DESC;
GO

-- 4. Price trends based on days left until departure
SELECT days_left, AVG(price) AS avg_ticket_price
FROM flight_data.flight_clean
GROUP BY days_left
ORDER BY days_left DESC; 
GO

/*
-- PHASE 8 BUSINESS-FOCUSED REPORTING INSIGHTS --
1. Top Expensive Flights: The absolute most expensive individual tickets are exclusively Vistara Business class flights operating between major hubs (Mumbai, Kolkata, Delhi).
2. Economy Champions: AirAsia stands out as the cheapest carrier on average for economy class, functioning strictly as a budget option.
3. Price Variability: Because premium carriers fly the same routes as budget carriers, all major city pairings exhibit massive price swings greater than 50,000.
4. Departure Trends: There is a clear, exponential price hike as the departure date approaches. Tickets purchased 1-3 days before departure cost significantly more than those booked 30+ days out.
*/
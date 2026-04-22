# Indian Flight Booking & Pricing Analysis (SQL)

## 📌 Project Overview
This project serves as an analytical SQL layer built on top of a flight booking dataset scraped from "Ease My Trip." It integrates multiple data files representing economy and business class flight options alongside a cleaned master dataset. The goal of this analysis is to demonstrate structured SQL reasoning, multi-table integration, and advanced query construction to uncover airline pricing strategies and route optimization metrics.

## 🛠️ Tech Stack
* **Database:** Microsoft SQL Server (T-SQL)
* **Techniques Used:** CTEs, Window Functions (`RANK`, `PARTITION BY`, `NTILE`), Multi-Table `JOIN`s, Conditional Logic (`CASE WHEN`), Data Type Casting, and String Scrubbing (`REPLACE`).

## 📊 Data Dictionary
The database consists of three primary tables containing approximately 300,000 total records:
* **`flight_clean`**: The primary merged dataset containing all combined flight booking records.
* **`flight_economy`**: A filtered dataset containing pricing and route information strictly for Economy class.
* **`flight_buisness`**: A filtered dataset containing pricing and route information strictly for Business class.

*(Note: Columns include flight id, airline, source_city, destination_city, departure_time, stops, class, days_left, and price).*

## 💡 Key Business Insights

### 1. The Premium Gap (Class Comparison)
By rigidly joining the Economy and Business tables, the data reveals that only two airlines (Vistara and Air India) operate a dual-class system. Vistara commands a massive premium, charging roughly 47,671 Rupees more for a Business class ticket compared to Economy (a ~600% markup).

### 2. Route Segmentation & Corporate Hubs
The Delhi-Mumbai corridor (in both directions) is the undisputed king of premium travel, boasting the highest volume of Business class flights in the market (over 5,000 each way). This heavy concentration indicates a massive corporate travel hub compared to southern routes like Chennai-Hyderabad.

### 3. Price Volatility
Because premium carriers fly the exact same routes as budget carriers (like AirAsia and SpiceJet), all major city pairings exhibit massive price swings, with variance gaps frequently exceeding 50,000 Rupees depending on the carrier and class chosen.

### 4. Departure Date Pricing Trends
There is a clear, exponential price hike as the departure date approaches. Analyzing the `days_left` timeline proves that tickets purchased 1-3 days before departure command the absolute highest prices, dropping significantly for bookings made 30+ days in advance.

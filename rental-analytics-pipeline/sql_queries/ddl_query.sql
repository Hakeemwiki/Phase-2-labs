
-------------------------------------------------------------------
-------------------------------------------------------------------
-- curated tables
DROP TABLE IF EXISTS curated.apartment_attributes;

CREATE TABLE curated.apartment_attributes (
    id INTEGER,
    category VARCHAR(50),
    body VARCHAR(256),
    amenities VARCHAR(255),
    bathrooms INTEGER,
    bedrooms INTEGER,
    fee DOUBLE PRECISION,
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_type VARCHAR(20),
    square_feet INTEGER,
    address VARCHAR(255),
    cityname VARCHAR(50),
    state VARCHAR(50),
    latitude NUMERIC(10,2),
    longitude NUMERIC(10,2),
    price DOUBLE PRECISION,
    price_display_new NUMERIC(10,2)
)
COMPOUND SORTKEY(id);




DROP TABLE IF EXISTS curated.apartments;

CREATE TABLE curated.apartments (
    id INT,
    title VARCHAR(255),
    source VARCHAR(255),
    price DOUBLE PRECISION,
    currency VARCHAR(10),
    listing_created_on DATE,
    is_active BOOLEAN,
    last_modified_timestamp DATE
);

DROP TABLE IF EXISTS curated.bookings;

CREATE TABLE curated.bookings (
    booking_id      VARCHAR(50),
    user_id         VARCHAR(50),
    apartment_id    VARCHAR(50),
    booking_date    DATE,
    checkin_date    DATE,
    checkout_date   DATE,
    total_price     DOUBLE PRECISION,
    currency        VARCHAR(10),
    booking_status  VARCHAR(20)
);

DROP TABLE IF EXISTS curated.user_viewing;

CREATE TABLE curated.user_viewing (
    user_id VARCHAR(255),
    apartment_id VARCHAR(255),
    viewed_at DATE,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(255)
);

-------------------------------------------------------------------
-------------------------------------------------------------------
-- apartments table
DROP TABLE IF EXISTS raws.apartments;

CREATE TABLE raws.apartments (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    source VARCHAR(50),
    price DECIMAL(10,2),
    currency VARCHAR(3),
    listing_created_on DATE,
    is_active BOOLEAN,
    last_modified_timestamp TIMESTAMP
);

-- apartment_attributes table
DROP TABLE IF EXISTS raws.apartment_attributes;

CREATE TABLE raws.apartment_attributes (
    id INT PRIMARY KEY,
    category VARCHAR(50),
    body TEXT,
    amenities VARCHAR(255),
    bathrooms INT,
    bedrooms INT,
    fee DECIMAL(10,2),
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display VARCHAR(255),
    price_type VARCHAR(50),
    square_feet INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8)
);

-- user_viewing table
DROP TABLE IF EXISTS raws.user_viewing;

CREATE TABLE raws.user_viewing (
    user_id INT,
    apartment_id INT,
    viewed_at VARCHAR(255),
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50)
);

-- bookings table
DROP TABLE IF EXISTS raws.bookings;

CREATE TABLE raws.bookings (
    booking_id INT PRIMARY KEY,
    user_id INT,
    apartment_id INT,
    booking_date VARCHAR(20),
    checkin_date VARCHAR(20),
    checkout_date VARCHAR(20),
    total_price DECIMAL(10,2),
    currency VARCHAR(3),
    booking_status VARCHAR(50)
);

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- 1. Average Listing Price Weekly
DROP TABLE IF EXISTS presentation.avg_listing_price_weekly;
CREATE TABLE presentation.avg_listing_price_weekly (
    week character varying(65535) ENCODE lzo,
    avg_listing_price double precision ENCODE raw
);

-- 2. Occupancy Rate Monthly
DROP TABLE IF EXISTS presentation.occupancy_rate_monthly;
CREATE TABLE presentation.occupancy_rate_monthly (
    month character varying(50) ENCODE lzo,
    nights_booked bigint ENCODE az64,
    total_available_nights bigint ENCODE az64,
    occupancy_rate numeric(21, 2) ENCODE az64
);

-- 3. Most Popular Locations Weekly
DROP TABLE IF EXISTS presentation.most_popular_locations_weekly;
CREATE TABLE presentation.most_popular_locations_weekly (
    week character varying(65535) ENCODE lzo,
    cityname character varying(100) ENCODE lzo,
    total_bookings bigint ENCODE az64
);

-- 4. Top Performing Listings Weekly
DROP TABLE IF EXISTS presentation.top_performing_listings_weekly;
CREATE TABLE presentation.top_performing_listings_weekly (
    week character varying(65535) ENCODE lzo,
    apartment_id character varying(65535) ENCODE lzo,
    total_revenue double precision ENCODE raw
);

-- 5. Total Bookings Per User Weekly
DROP TABLE IF EXISTS presentation.total_bookings_per_user_weekly;
CREATE TABLE presentation.total_bookings_per_user_weekly (
    week character varying(65535) ENCODE lzo,
    --user_id INTEGER,
    total_bookings bigint ENCODE az64
);

-- 6. Average Booking Duration Weekly
DROP TABLE IF EXISTS presentation.avg_booking_duration_weekly;
CREATE TABLE presentation.avg_booking_duration_weekly (
    week character varying(65535) ENCODE lzo,
    avg_booking_duration numeric(20, 0) ENCODE az64
);

-- 7. Repeat Customer Rate Monthly
DROP TABLE IF EXISTS presentation.repeat_customer_rate_monthly;
CREATE TABLE presentation.repeat_customer_rate_monthly (
    month character varying(65535) ENCODE lzo,
    repeat_customers bigint ENCODE az64
);
-- Create schemas
CREATE SCHEMA IF NOT EXISTS prod;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS marts;

-- Instanciate production tables
SET search_path TO prod;

CREATE TABLE IF NOT EXISTS aisles (
    aisle_id bigint primary key NOT NULL,
    aisle text NOT NULL
);

CREATE TABLE IF NOT EXISTS departments (
    department_id bigint primary key NOT NULL,
    department text NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id bigint primary key NOT NULL,
    product_name text NOT NULL,
    aisle_id integer REFERENCES aisles (aisle_id),
    department_id integer REFERENCES departments (department_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id bigint primary key NOT NULL,
    user_id integer NOT NULL,
    order_number integer NOT NULL,
    order_dow integer NOT NULL,
    order_hour_of_day integer,
    days_since_prior_order integer
)

CREATE TABLE IF NOT EXISTS purchases (
    order_id integer REFERENCES orders (order_id),
    product_id integer REFERENCES products (product_id),
    add_to_cart_order integer NOT NULL,
    reordered integer NOT NULL
);
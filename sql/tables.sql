CREATE TABLE sales_calculated(
sale_id SERIAL primary KEY,
sale_date DATE NOT NULL,
price DECIMAL(10,2) NOT NULL,
quantity INT NOT null,
client_id INT NOT null,
category VARCHAR(255) NOT null,
total_sales DECIMAL(10,2) NOT NULL);

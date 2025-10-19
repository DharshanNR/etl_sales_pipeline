import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# --- Configuration ---
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 500
NUM_ORDERS = 5000
MAX_ITEMS_PER_ORDER = 5
MIN_QUANTITY_PER_ITEM = 1
MAX_QUANTITY_PER_ITEM = 3
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 3, 31)

# --- Data Generation Functions ---

def generate_customers(num_customers):
    customers_data = []
    for _ in range(num_customers):
        customer_id = fake.uuid4()
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        country = fake.country()
        customers_data.append({
            'customer_id': customer_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'country': country
        })
    return pd.DataFrame(customers_data)

def generate_products(num_products):
    products_data = []
    categories = ['Electronics', 'Clothing', 'Home Goods', 'Books', 'Beauty', 'Sports', 'Food', 'Toys']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'Generic']
    for i in range(num_products):
        product_id = fake.uuid4()
        product_name = fake.catch_phrase() + f" {i+1}" # Ensure unique names
        category = random.choice(categories)
        brand = random.choice(brands)
        products_data.append({
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'brand': brand
        })
    return pd.DataFrame(products_data)

def generate_orders_and_items(num_orders, customers_df, products_df, start_date, end_date):
    orders_data = []
    order_items_data = []
    order_statuses = ['completed', 'pending', 'cancelled']

    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()

    for _ in range(num_orders):
        order_id = fake.uuid4()
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        status = random.choice(order_statuses)
        
        total_amount = 0
        num_items_in_order = random.randint(1, MAX_ITEMS_PER_ORDER)
        
        # Ensure unique products within an order
        selected_products_for_order = random.sample(product_ids, min(num_items_in_order, len(product_ids)))

        for product_id in selected_products_for_order:
            order_item_id = fake.uuid4()
            quantity = random.randint(MIN_QUANTITY_PER_ITEM, MAX_QUANTITY_PER_ITEM)
            price_per_unit = round(random.uniform(5.0, 500.0), 2) # Random price
            
            item_total = quantity * price_per_unit
            total_amount += item_total

            order_items_data.append({
                'order_item_id': order_item_id,
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'price_per_unit': price_per_unit
            })
        
        orders_data.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'), # Format date for CSV
            'total_amount': round(total_amount, 2),
            'status': status
        })

    return pd.DataFrame(orders_data), pd.DataFrame(order_items_data)

# --- Generate DataFrames ---
print("Generating customers...")
customers_df = generate_customers(NUM_CUSTOMERS)

print("Generating products...")
products_df = generate_products(NUM_PRODUCTS)

print("Generating orders and order items...")
orders_df, order_items_df = generate_orders_and_items(NUM_ORDERS, customers_df, products_df, START_DATE, END_DATE)

# --- Save to CSV ---
print("Saving data to CSV files...")
customers_df.to_csv('data\customers.csv', index=False)
products_df.to_csv('data\products.csv', index=False)
orders_df.to_csv('data\orders.csv', index=False)
order_items_df.to_csv('data\order_items.csv', index=False)

print("Data generation complete!")
print(f"Generated {len(customers_df)} customers.")
print(f"Generated {len(products_df)} products.")
print(f"Generated {len(orders_df)} orders.")
print(f"Generated {len(order_items_df)} order items.")
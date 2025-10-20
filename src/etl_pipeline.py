# etl_pipeline.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    year,
    month,
    dayofweek,
    sum,
    avg,
    count,
    round,
    lower,
    trim,
    rank,
    dense_rank,
    lag,
    lit,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
    TimestampType,
)
import os

# --- 1. Initialize Spark Session ---
def initialize_spark_session(app_name="EcommerceETLPipeline"):
    """Initializes and returns a SparkSession."""
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") # For older Spark versions or specific date formats
        .getOrCreate()
    )
    return spark

# --- 2. Data Loading & Initial Exploration ---
def load_data(spark, input_path):
    """
    Loads CSV files into PySpark DataFrames and performs initial exploration.
    """
    print(f"\n--- Loading Data from {input_path} ---")

    # Define schemas for better control and performance
    # customers.csv: customer_id (string), first_name (string), last_name (string), email (string), country (string)
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
    ])

    # products.csv: product_id (string), product_name (string), category (string), brand (string)
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
    ])

    # orders.csv: order_id (string), customer_id (string), order_date (date), total_amount (float), status (string)
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True), # Read as string first for flexible parsing
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ])

    # order_items.csv: order_item_id (string), order_id (string), product_id (string), quantity (integer), price_per_unit (float)
    order_items_schema = StructType([
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price_per_unit", DoubleType(), True),
    ])

    # Load DataFrames
    customers_df = spark.read.csv(os.path.join(input_path, "customers.csv"), header=True, schema=customers_schema)
    products_df = spark.read.csv(os.path.join(input_path, "products.csv"), header=True, schema=products_schema)
    orders_df = spark.read.csv(os.path.join(input_path, "orders.csv"), header=True, schema=orders_schema)
    order_items_df = spark.read.csv(os.path.join(input_path, "order_items.csv"), header=True, schema=order_items_schema)

    # Basic Exploration
    print("\n--- Customers DataFrame ---")
    customers_df.printSchema()
    customers_df.show(5)
    print(f"Customer Count: {customers_df.count()}")

    print("\n--- Products DataFrame ---")
    products_df.printSchema()
    products_df.show(5)
    print(f"Product Count: {products_df.count()}")

    print("\n--- Orders DataFrame ---")
    orders_df.printSchema()
    orders_df.show(5)
    print(f"Order Count: {orders_df.count()}")

    print("\n--- Order Items DataFrame ---")
    order_items_df.printSchema()
    order_items_df.show(5)
    print(f"Order Item Count: {order_items_df.count()}")

    return customers_df, products_df, orders_df, order_items_df

# --- 3. Data Cleaning & Transformation ---
def clean_and_transform_data(customers_df, products_df, orders_df, order_items_df):
    """
    Performs data cleaning, type conversion, feature engineering, and joins.
    """
    print("\n--- Cleaning & Transforming Data ---")

    # --- Data Type Conversion & Missing Values ---

    # Orders DataFrame: Convert order_date to TimestampType
    # Assuming format 'YYYY-MM-DD HH:MM:SS'
    orders_df = orders_df.withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"))

    # Handle potential nulls in total_amount (fill with 0 or drop)
    # For this dataset, we expect total_amount to be calculated, so we'll rely on that.
    # If it were from an external source, we might fill with 0:
    # orders_df = orders_df.na.fill(0.0, subset=["total_amount"])

    # Order Items DataFrame: Ensure quantity and price_per_unit are correct types
    # (already defined in schema, but good to re-check if schema inference was used)
    order_items_df = order_items_df.withColumn("quantity", col("quantity").cast(IntegerType()))
    order_items_df = order_items_df.withColumn("price_per_unit", col("price_per_unit").cast(DoubleType()))

    # Handle potential nulls in quantity or price_per_unit (e.g., fill with 0, drop rows)
    # For now, we'll drop rows with nulls in these critical columns for item_total calculation
    initial_order_items_count = order_items_df.count()
    order_items_df = order_items_df.na.drop(subset=["quantity", "price_per_unit"])
    if order_items_df.count() < initial_order_items_count:
        print(f"Dropped {initial_order_items_count - order_items_df.count()} rows from order_items due to null quantity or price_per_unit.")

    # --- Derive New Features ---

    # Calculate item_total in order_items
    order_items_df = order_items_df.withColumn("item_total", round(col("quantity") * col("price_per_unit"), 2))

    # Extract year, month, day_of_week from order_date
    orders_df = orders_df.withColumn("order_year", year(col("order_date")))
    orders_df = orders_df.withColumn("order_month", month(col("order_date")))
    orders_df = orders_df.withColumn("order_day_of_week", dayofweek(col("order_date"))) # 1=Sunday, 7=Saturday

    # --- Join DataFrames to create a wide fact table ---

    # 1. Join orders with order_items on order_id
    # Use 'inner' join to keep only matching records
    fact_df = orders_df.join(order_items_df, on="order_id", how="inner")

    # 2. Join with products on product_id
    fact_df = fact_df.join(products_df, on="product_id", how="inner")

    # 3. Join with customers on customer_id
    fact_df = fact_df.join(customers_df, on="customer_id", how="inner")

    # --- Filter Data ---
    # Filter out cancelled orders
    initial_fact_count = fact_df.count()
    fact_df = fact_df.filter(col("status") != "cancelled")
    print(f"Filtered out {initial_fact_count - fact_df.count()} rows (cancelled orders).")

    # Filter out orders with invalid total_amount (e.g., negative or zero if not allowed)
    fact_df = fact_df.filter(col("total_amount") > 0)
    print(f"Filtered out orders with total_amount <= 0. Remaining rows: {fact_df.count()}")

    # --- Standardize Data (Optional) ---
    # Convert text fields to lowercase, trim whitespace
    fact_df = fact_df.withColumn("product_name", lower(trim(col("product_name"))))
    fact_df = fact_df.withColumn("category", lower(trim(col("category"))))
    fact_df = fact_df.withColumn("brand", lower(trim(col("brand"))))
    fact_df = fact_df.withColumn("status", lower(trim(col("status"))))
    fact_df = fact_df.withColumn("country", lower(trim(col("country"))))

    print("\n--- Transformed Fact Table Schema ---")
    fact_df.printSchema()
    fact_df.show(5)
    print(f"Final Fact Table Count: {fact_df.count()}")

    return fact_df

# --- 4. Data Aggregation & Analysis ---
def perform_analysis(fact_df):
    """
    Calculates various KPIs and performs aggregations.
    """
    print("\n--- Performing Data Aggregation & Analysis ---")

    # --- KPIs ---

    # Total Sales by Date
    sales_by_date = fact_df.groupBy(to_date(col("order_date")).alias("sale_date")) \
                           .agg(round(sum("item_total"), 2).alias("daily_total_sales")) \
                           .orderBy("sale_date")
    print("\nTotal Sales by Date:")
    sales_by_date.show(5)

    # Total Sales by Product Category
    sales_by_category = fact_df.groupBy("category") \
                               .agg(round(sum("item_total"), 2).alias("category_total_sales")) \
                               .orderBy(col("category_total_sales").desc())
    print("\nTotal Sales by Product Category:")
    sales_by_category.show(5)

    # Top N Products (by item_total)
    top_n_products = fact_df.groupBy("product_id", "product_name") \
                            .agg(round(sum("item_total"), 2).alias("product_total_sales")) \
                            .orderBy(col("product_total_sales").desc()) \
                            .limit(10)
    print("\nTop 10 Products by Sales:")
    top_n_products.show()

    # Top N Customers (by total_amount spent)
    top_n_customers = fact_df.groupBy("customer_id", "first_name", "last_name") \
                             .agg(round(sum("item_total"), 2).alias("customer_total_spent")) \
                             .orderBy(col("customer_total_spent").desc()) \
                             .limit(10)
    print("\nTop 10 Customers by Spending:")
    top_n_customers.show()

    # Monthly Sales Trends
    monthly_sales = fact_df.groupBy("order_year", "order_month") \
                           .agg(round(sum("item_total"), 2).alias("monthly_sales")) \
                           .orderBy("order_year", "order_month")
    print("\nMonthly Sales Trends:")
    monthly_sales.show(5)

    # Average Order Value (AOV)
    # Note: AOV is typically calculated per order, not per item in the fact table.
    # We need to group by order_id first to get order total, then average those.
    order_totals_df = fact_df.groupBy("order_id").agg(round(sum("item_total"), 2).alias("order_total"))
    aov = order_totals_df.agg(round(avg("order_total"), 2).alias("average_order_value"))
    print("\nAverage Order Value (AOV):")
    aov.show()

    # Sales by Country
    sales_by_country = fact_df.groupBy("country") \
                              .agg(round(sum("item_total"), 2).alias("country_total_sales")) \
                              .orderBy(col("country_total_sales").desc())
    print("\nSales by Country:")
    sales_by_country.show(5)

    # --- Window Functions (Advanced) ---

    # Running Totals of Sales by Date
    window_spec_date = Window.orderBy(to_date(col("sale_date")))
    running_total_sales = fact_df.groupBy(to_date(col("order_date")).alias("sale_date")) \
                                 .agg(round(sum("item_total"), 2).alias("daily_sales")) \
                                 .withColumn("running_total_sales", round(sum("daily_sales").over(window_spec_date), 2)) \
                                 .orderBy("sale_date")
    print("\nRunning Total Sales by Date:")
    running_total_sales.show(5)

    # Rank products within categories by sales
    window_spec_category = Window.partitionBy("category").orderBy(col("category_product_sales").desc())
    ranked_products_in_category = fact_df.groupBy("category", "product_id", "product_name") \
                                         .agg(round(sum("item_total"), 2).alias("category_product_sales")) \
                                         .withColumn("rank_in_category", rank().over(window_spec_category)) \
                                         .filter(col("rank_in_category") <= 3)\
                                         .orderBy("category", "rank_in_category")
    print("\nTop 3 Products within Each Category by Sales:")
    ranked_products_in_category.show(10, truncate=False)

    # Example: Sales growth month-over-month
    window_spec_monthly = Window.orderBy("order_year", "order_month")
    monthly_sales_with_lag = monthly_sales.withColumn("prev_month_sales", lag("monthly_sales", 1).over(window_spec_monthly)) \
                                          .withColumn("mom_growth_percent",
                                                      round(((col("monthly_sales") - col("prev_month_sales")) / col("prev_month_sales")) * 100, 2))
    print("\nMonthly Sales with Month-over-Month Growth:")
    monthly_sales_with_lag.show()

    return {
        "sales_by_date": sales_by_date,
        "sales_by_category": sales_by_category,
        "top_n_products": top_n_products,
        "top_n_customers": top_n_customers,
        "monthly_sales": monthly_sales,
        "aov": aov,
        "sales_by_country": sales_by_country,
        "running_total_sales": running_total_sales,
        "ranked_products_in_category": ranked_products_in_category,
        "monthly_sales_with_lag": monthly_sales_with_lag,
    }


# --- 5. Data Storage (PySpark to Parquet) ---
def store_processed_data(fact_df, analysis_results, output_path):
    """
    Stores the cleaned, transformed, and aggregated DataFrames into CSV format.
    Each DataFrame will be written to its own subdirectory within the output_path.
    """
    print(f"\n--- Storing Processed Data to {output_path} (CSV format) ---")

    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)

    # Store the main fact table
    fact_table_output_dir = os.path.join(output_path, "fact_table_csv")
    print(f"Storing fact_table.csv to {fact_table_output_dir}...")
    # For CSV, Spark writes multiple part files. We'll write to a subdirectory.
    fact_df.toPandas().to_csv(fact_table_output_dir, index=True,mode='w')
    #fact_df.repartition(1).write.mode("overwrite").csv(fact_table_output_dir, header=True)

    # Store partitioned fact table (example: by year and month)
    # Note: Partitioning for CSV in Spark creates subdirectories for each partition key.
    #partitioned_fact_table_output_dir = os.path.join(output_path, "fact_table_partitioned_csv")
    #print(f"Storing fact_table_partitioned.csv to {partitioned_fact_table_output_dir} (partitioned by order_year, order_month)...")
    #fact_df.write.mode("overwrite").partitionBy("order_year", "order_month").csv(
     #   partitioned_fact_table_output_dir, header=True
    #)'''

    # Store aggregated results
    for name, df in analysis_results.items():
        aggregated_output_dir = os.path.join(output_path, f"{name}_csv")
        print(f"Storing {name}.csv to {aggregated_output_dir}...")
        df.toPandas().to_csv(aggregated_output_dir, header=True,mode='w')

    print("Data storage complete!")

# --- Main ETL Pipeline Execution ---
if __name__ == "__main__":
    spark = None # Initialize spark to None
    try:
        spark = initialize_spark_session()
        input_data_path = "data/raw/"  
        output_data_path = "./output"

        # 1. Load Raw Data
        customers_df, products_df, orders_df, order_items_df = load_data(spark, input_data_path)

        # 2. Clean & Transform Data
        transformed_fact_df = clean_and_transform_data(customers_df, products_df, orders_df, order_items_df)

        # 3. Data Aggregation & Analysis
        analysis_results = perform_analysis(transformed_fact_df)

        # 4. Data Storage
        store_processed_data(transformed_fact_df, analysis_results, output_data_path)

    except Exception as e:
        print(f"An error occurred during the ETL pipeline: {e}")
    finally:
        if spark:
            spark.stop()

            print("\nSpark Session Stopped.")

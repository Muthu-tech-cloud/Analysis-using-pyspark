from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("CSV Reader").master("local[*]").getOrCreate()

# Use DataFrame API
products_df = spark.read.csv(r"Product.csv", header=True, inferSchema=True)
products_df.dropna(how='all')
#products_df.show()
#products_df.printSchema()
sales_df = spark.read.csv(r"Sales.csv", header=True, inferSchema=True)
sales_df.dropna(how='all')
#sales_df.show()
#sales_df.printSchema()
customer_df = spark.read.csv(r"Customer.csv", header=True, inferSchema=True)
customer_df.dropna(how='all')
#customer_df.show()
#customer_df.printSchema()

customer_sales_df = customer_df.join(sales_df, on='Customer ID', how='inner')

joined_df = customer_sales_df.join(products_df, on='Product ID', how='inner')

#joined_df.show(5,truncate=False)
joined_df.printSchema()

# What is the total sales for each product category
TotalQuantity = joined_df.groupBy('Category').agg(F.sum("Quantity").alias("TotalQuantity"))
#TotalQuantity.show()
# Which customer has made the highest number of purchases
customer_purchase = joined_df.groupBy('Customer Name').agg(F.count('Order ID').alias('TotalCount'))
#customer_purchase.show()
# What is the average discount given on sales across all products
avg_discounts = joined_df.groupBy('Product Name').agg(F.avg('Discount').alias('Average_Discounts'))
#avg_discounts.show()
#How many unique products were sold in each region
unique_product = joined_df.select('Product Name').distinct()
#unique_product.show()
# What is the total profit generated in each state
total_profit_by_state = joined_df.groupBy('State').agg(F.sum('Profit').alias('Total_Profit'))
# total_profit_by_state.show()
# Which product sub-category has the highest sales
highest_sale = joined_df.groupBy('Sub-Category').agg(F.sum("Sales").alias("HighestSales"))
#highest_sale.show()
# What is the average age of customers in each segment
avg_customer = joined_df.groupBy('Segment').agg(F.avg('Age').alias('AverageAge'))
#avg_customer.show()
# How many orders were shipped in each shipping mode
orders_shipped = joined_df.groupBy('Ship Mode').agg(F.count('Order ID').alias('TotalOrders'))
#orders_shipped.show()
# What is the total quantity of products sold in each city
total_products_by_city = joined_df.groupBy('City').agg(F.sum('Quantity').alias('TotalQuantity'))
total_products_by_city.show()
# Which customer segment has the highest profit margin
profit_margin = joined_df.groupBy('Segment').agg(F.sum('Profit').alias('TotalProfit')).orderBy(F.desc('TotalProfit'))
profit_margin.show(1)

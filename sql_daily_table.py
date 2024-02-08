# Databricks notebook source
#-----------------------------------------
# Set the prod folder path
#-----------------------------------------
prod_folder_path = "/mnt/prod/"

stocks = ["aapl", "amzn", "googl", "msft"]

# COMMAND ----------

#-----------------------------------------
# Create the daily table for each stock
#-----------------------------------------
for stock in stocks:
    # Create the path for the stock
    path = f'{prod_folder_path}{stock}/year=*/month=*/*'
    # Load all the parquet files at once
    df = spark.read.parquet(path)
    
    # Check if the table exists
    if spark.catalog.tableExists(stock):
        # Drop the existing table
        spark.sql(f"DROP TABLE {stock}")
        print(f'Dropped table: {stock}')
    
    # Create the table
    df.write.format("parquet").saveAsTable(stock)
    print(f'Table for {stock} is created')

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Display the table for apple stock
# MAGIC
# MAGIC SELECT *
# MAGIC FROM aapl
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(date), avg(close) as mean_stock_price_by_year
# MAGIC FROM aapl
# MAGIC GROUP BY year(date)
# MAGIC ORDER BY year(date)

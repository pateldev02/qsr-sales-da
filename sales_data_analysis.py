#!/usr/bin/env python
# coding: utf-8


#Sales Dataframe
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import functions as F

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("cust_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("source_order", StringType(), True),
])

# Load the CSV file with inferred schema and defined schema
sales_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .schema(schema) \
    .load(r"E:/code-mine/QSR_Sales_DA/salesDataProcessing/sales_data.csv")

# Display the dataframe
sales_df.show()


# In[27]:


#Adding Columns

from pyspark.sql.functions import month, year, quarter

sales_df = sales_df.withColumn("order_month", month(sales_df.order_date))
sales_df = sales_df.withColumn("order_quarter", quarter(sales_df.order_date))
sales_df = sales_df.withColumn("order_year", year(sales_df.order_date))
display(sales_df)
sales_df.show()


# In[9]:


#Menu Dataframe
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", StringType(), True),
])

# Load the CSV file with inferred schema and defined schema
menu_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .schema(schema) \
    .load(r"E:/code-mine/QSR_Sales_DA/salesDataProcessing/menu_data.csv")

# Display the dataframe
menu_df.show()


# In[29]:


#Total amount spent by customer

total_amount_spend = (sales_df.join(menu_df, 'product_id')
                      .groupBy('cust_id')
                      .agg({'price':'sum'})
                      .orderBy('cust_id')
                      .withColumn('sum(price)', F.format_number('sum(price)', 2))
                     )

display(total_amount_spend)
total_amount_spend.show()


# In[30]:


#Total amount spent by food categorization

total_amount_spend_by_food = (sales_df.join(menu_df, 'product_id')
                      .groupBy('product_name')
                      .agg({'price':'sum'})
                      .orderBy('product_name')
                              .withColumn('sum(price)', F.format_number('sum(price)', 2))
                     )

display(total_amount_spend)
total_amount_spend.show()


# In[22]:


#Total sales by each month

monthly_sales = (sales_df.join(menu_df, 'product_id')
                      .groupBy('order_month')
                      .agg({'price':'sum'})
                      .orderBy('order_month')
                      .withColumn('sum(price)', F.format_number('sum(price)', 2))
                     )

monthly_sales.show()


# In[ ]:





# In[39]:


#Product purchasing frequency

from pyspark.sql.functions import count

purchase_freq = (sales_df.join(menu_df, 'product_id')
                .groupBy('product_name')
                .agg(count('product_id').alias('product_count'))
                .orderBy('product_name', ascending = 0)
                 .drop('product_id')
                )

purchase_freq.show()


# In[40]:


#Top 5 most frequently bought products

from pyspark.sql.functions import count

purchase_freq = (sales_df.join(menu_df, 'product_id')
                .groupBy('product_id', 'product_name')
                .agg(count('product_id').alias('product_count'))
                .orderBy('product_count', ascending=False)  # Order by count, not product name
                .limit(5)  # Top 5
                )

purchase_freq.show()


# In[16]:


#Customer Frequency

from pyspark.sql.functions import countDistinct as cd

cust_freq = (sales_df.filter(sales_df.source_order == 'Online')
     .groupBy('cust_id')
     .agg(cd('order_date'))
     .orderBy('cust_id', ascending = 1)
     )

cust_freq.show()


# In[15]:


#Sales by country

from pyspark.sql import functions as F

sales_by_country = (sales_df.join(menu_df, 'product_id')
                   .groupBy('location')
                   .agg({'price': 'sum'})
                   .withColumn('sum(price)', F.format_number('sum(price)', 2))
                   )

sales_by_country.show()


# In[41]:


#Sales by platform


sales_by_platform =  (sales_df.join(menu_df, 'product_id')
                   .groupBy('source_order')
                   .agg({'price': 'sum'})
                   .withColumn('sum(price)', F.format_number('sum(price)', 2))
                   )

sales_by_platform.show()


# In[ ]:





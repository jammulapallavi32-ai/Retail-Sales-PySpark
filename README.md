# Retail Sales Analysis using PySpark

## Project Overview
This project demonstrates how to analyze retail sales data using PySpark. It performs data cleaning, transformations, aggregations, and window function operations on a retail sales dataset.

## Technologies Used
- Python 3
- PySpark
- Apache Spark
- VS Code

## Dataset Columns
- Order_ID
- Order_Date
- Customer
- City
- Category
- Product
- Quantity
- Price

## Features
- Read CSV file using PySpark
- Create a new column (Total_Amount)
- Calculate total sales by city
- Calculate total sales by category
- Find top-selling products
- Find monthly sales
- Find the highest spending customer
- Apply Window Functions:
  - Row Number
  - Rank
  - Dense Rank
  - Partition By

## Project Structure
```
Retail-Sales-PySpark/
│── data/
│   └── retail_sales.csv
│── main.py
│── requirements.txt
│── README.md
```

## How to Run

1. Clone the repository
2. Install PySpark

```bash
pip install pyspark
```

3. Run the project

```bash
python main.py
```

## Sample Output
- Dataset Preview
- Total Sales by City
- Total Sales by Category
- Top Selling Products
- Monthly Sales
- Highest Spending Customer
- Window Function Results

## Note
The output writing step is currently commented out because `winutils.exe` / `HADOOP_HOME` is not configured on Windows. All data analysis and transformations run successfully.

## Learning Outcomes
- SparkSession
- DataFrame API
- Data Transformation
- Aggregations
- Window Functions
- Partitioning
- Sorting
- PySpark Basics

## Author
**Pallavi Jammula**
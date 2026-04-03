# 🚀 Phase 3A – Data Quality & Cleaning Challenge (PySpark)

---

## 🔹 Objective

This phase focuses on working with intentionally messy data and applying proper **data cleaning techniques using PySpark** before performing any analysis.

The goal is to understand why **data quality is critical in real-world data engineering pipelines**.

---

## ⏱ Expected Effort

30–45 minutes

---

## 📚 Mandatory Reading

- https://www.sparkplayground.com/tutorials/pyspark/filtering-data  
- https://www.sparkplayground.com/tutorials/pyspark/grouping-data  

---

## 🔹 Problem Statement

You are given a messy dataset containing inconsistent and invalid records.

### 📌 Dataset

```python
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

⚠️ Identified Data Issues
Missing values (name, city, customer_id)
Duplicate records
Invalid values (negative age)
Primary key issues (customer_id null)
🔹 Tasks
1. Identify Data Issues
Detect null values
Find duplicate rows
Identify invalid age values
2. Clean the Data

Apply the following cleaning steps:

Remove rows with null customer_id
Handle missing values (name, city)
Remove duplicate records
Filter invalid age values (age > 0)
3. Validate Cleaning
Compare row count before and after cleaning
Ensure data integrity is maintained
4. Aggregation
Group data by city
Count number of customers per city
🔹 Key PySpark Operations Used
createDataFrame()
dropna()
fillna()
dropDuplicates()
filter()
groupBy()
count()
printSchema()
🔹 Key Learnings
Real-world data is always messy
Data cleaning is mandatory before analysis
Invalid data leads to incorrect results
Validation ensures data reliability and correctness
🔹 Reflection Questions
❓ What happens if cleaning is skipped?
Incorrect insights
Wrong aggregations
Poor business decisions
❓ Which issue impacted results most?
Duplicate records and invalid values had the highest impact on correctness
❓ How would this affect business decisions?
Leads to misleading analytics and incorrect strategic decisions
📌 Cleaning Checklist
Remove null primary keys
Handle missing values
Remove duplicates
Validate numeric ranges (e.g., age > 0)
Verify schema consistency
Validate results after transformation
🔹 Conclusion

This challenge demonstrates the importance of data cleaning in real-world data engineering pipelines.

It highlights how raw, messy data must be transformed into clean and reliable datasets before performing any business analysis or building data pipelines.

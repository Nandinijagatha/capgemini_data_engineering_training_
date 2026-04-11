# 📊 PySpark Data Cleaning & Transformation Practice

This notebook demonstrates fundamental **data cleaning and transformation techniques using PySpark**. It covers real-world preprocessing steps such as handling duplicates, null values, formatting inconsistencies, and data standardization.

---

## 🚀 Project Overview

The notebook performs the following operations:

- Initialize Spark Session
- Load CSV data
- Remove duplicate records
- Handle missing and invalid values
- Standardize text data
- Perform column transformations
- Work with date parsing and error handling

---

## 🛠️ Technologies Used

- Python
- PySpark

---

## 📂 Dataset

- Input file: `customers.csv`
- Loaded using Spark DataFrame with schema inference

---

## 🔄 Workflow Steps

### 1. Initialize Spark Session
- Create a Spark session to start working with PySpark.

### 2. Load Data
- Read CSV file with header and inferred schema.

### 3. Remove Duplicates
- Use `dropDuplicates()` to eliminate duplicate rows.

### 4. Handle Invalid Values
- Replace `"blank"` or `"Blank"` with `NULL`.

### 5. Standardize Text Data
- Convert `Country` column values to uppercase.

### 6. Data Cleaning
- Replace inconsistent values (e.g., `"NEW YORK"` → `"USA"`).

### 7. Handle Missing Values
- Fill null values:
  - `Category` → `"Unknown"`
  - `Sales` → `0`
  - `JoinDate` → `"2023-01-01"`

### 8. Date Handling & Validation
- Use `try_to_date()` to safely parse dates.
- Invalid dates are converted to `NULL`.

### 9. Sample Data Processing
- Create a sample DataFrame.
- Perform:
  - Duplicate removal
  - Null handling
  - Date cleaning

---

## 📌 Key PySpark Functions Used

- `read.format()`
- `dropDuplicates()`
- `replace()`
- `withColumn()`
- `upper()`
- `regexp_replace()`
- `fillna()`
- `try_to_date()`

---

## 📈 Learning Outcomes

- Understand basic data cleaning techniques in PySpark
- Handle real-world dirty data
- Perform efficient transformations on large datasets
- Improve data quality for downstream analytics

---

## ▶️ How to Run

1. Install PySpark
2. Place dataset in the correct path
3. Run the notebook step by step

---

## 📎 Notes

- Ensure file paths are correctly set
- Handle schema carefully for large datasets
- Use `display()` or `show()` to visualize data

---

## ✨ Author

- NANDINI JAGATHA

Phase 6 – SQL Joins, Aggregations & Data Analysis

🔹 Objective

In this phase, the goal is to develop a strong understanding of SQL joins, grouping, and data analysis techniques by working with multiple related datasets.

This phase focuses on:

Combining data from multiple tables
Applying aggregations using GROUP BY
Solving real-world SQL problems
Understanding data relationships

🔹 Problem Summary

We were given multiple relational tables such as:

employees
departments
projects
salary (optional dataset)

The task was to:

Perform different types of joins
Apply grouping and aggregation operations
Solve 30 SQL practice questions
Handle missing and inconsistent data

🔹 Approach

Analyzed table structures and relationships
Identified keys (primary & foreign keys)
Applied appropriate join types based on use case
Used GROUP BY for aggregations
Applied filtering conditions using WHERE and HAVING
Handled NULL values and data inconsistencies
Validated outputs with sample data

🔹 Key Concepts & Transformations Used

 Joins

INNER JOIN → returns only matching records
LEFT JOIN → returns all records from left table
RIGHT JOIN → returns all records from right table
FULL OUTER JOIN → returns all records from both tables
SELF JOIN → used to join a table with itself (e.g., employee-manager relationship)

GROUP BY & Aggregations (Core Concept)

GROUP BY → groups rows based on column values
COUNT(*) → counts total records
SUM() → calculates total values
AVG() → calculates average

Example:

SELECT department, COUNT(*) AS emp_count
FROM employees
GROUP BY department;

 Used for:

Department-wise employee count
Salary aggregation
Project analysis

 Filtering

WHERE → filters rows before grouping
HAVING → filters results after aggregation

 Data Cleaning Techniques

Removed null values using filtering
Handled missing relationships using joins
Removed duplicate records
Fixed incorrect or inconsistent data
Ensured proper data types

🔹 Output / Results

The following outputs were generated:

Employee–Manager hierarchy
Employees with and without departments
Projects with and without assigned employees
Department-wise employee counts
Aggregated salary insights
Employees without projects or departments

🔹 Data Engineering Considerations

Used correct join types to avoid data loss
Handled NULL values carefully to maintain accuracy
Avoided duplicate records during joins
Ensured aggregations were applied correctly
Validated results using sample datasets

🔹 Challenges Faced

Choosing the correct join type for each scenario
Handling NULL values in joins and aggregations
Understanding difference between WHERE and HAVING
Writing queries for complex real-world problems
Implementing self joins correctly

🔹 Learnings

Strong understanding of SQL joins and relationships
Practical knowledge of GROUP BY and aggregations
Ability to solve real-world SQL problems
Clear understanding of filtering techniques
Importance of data cleaning before analysis
Introduction to window functions

🔹 Author

NANDINI JAGATHA

Phase – SQL Conditional Logic & Window Functions

🔹 Objective
Understand and apply SQL conditional logic and window functions for advanced data analysis.

This phase focuses on:

Writing conditional transformations
Categorizing data based on business rules
Performing ranking and ordering operations

🔹 Problem Summary
Worked on datasets such as:

Employee dataset
CustomerUsage dataset
Orders dataset

Tasks included:

Applying conditional logic
Solving multi-level conditions
Using window functions for ranking
Performing group-based analysis

🔹 Approach

Analyzed datasets and identified required transformations
Applied conditional logic for data categorization
Used nested conditions for complex scenarios
Implemented window functions for ranking
Applied grouping and sorting techniques
Validated outputs with sample data

🔹 Key Transformations Used

🔸 Conditional Logic

Used for applying business rules
Helps categorize data

Example:

CASE WHEN salary > 80000 THEN 'High' ELSE 'Low' END

🔸 Nested Conditions

Used for handling multiple dependent conditions
Useful for complex logic implementation

🔸 Window Functions

ROW_NUMBER() → Unique row numbering
RANK() → Ranking with gaps
DENSE_RANK() → Ranking without gaps

🔸 Window Clauses

PARTITION BY → Groups data logically
ORDER BY → Defines sorting within groups

🔹 Output / Results

Employee ranking based on salary
Department-wise ranking
Customer categorization
Bonus and salary calculations
Identification of high-value and low-value users

🔹 Data Engineering Considerations

Maintained correct order of conditions
Avoided overlapping logic
Used grouping clauses appropriately
Defined sorting explicitly
Verified correctness of ranking outputs

🔹 Challenges Faced

Handling multi-level conditions
Understanding ranking differences
Writing accurate logical expressions
Managing multiple conditions in queries

🔹 Learnings

Strong understanding of conditional logic
Practical implementation of business rules
Clear knowledge of ranking functions
Better understanding of grouping vs partitioning
Improved SQL problem-solving skills

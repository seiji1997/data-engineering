# Data Engineering: SQL vs Pandas

## 1. Replace Null Values

Pandas

```python
df['col1'].fillna(0)
```

SQL
```sql
SELECT COALESCE(col1, 0) FROM table;
```

Explanation:

In Pandas, fillna(0) replaces NaN values in col1 with 0.
In SQL, COALESCE(col1, 0) returns 0 if col1 is NULL.

## 2. Calculate the Difference Between Two Columns
Pandas:

python

df['col1'] - df['col2']
SQL:

sql

SELECT col1 - col2 FROM table;
Explanation:

Both Pandas and SQL allow subtraction of two columns directly to get the difference.
3. Concatenate Two Columns
Pandas:

python

df['col1'] + df['col2']
SQL:

sql

SELECT CONCAT(col1, col2) FROM table;
Explanation:

In Pandas, using the + operator concatenates strings.
In SQL, CONCAT function is used to concatenate two strings.
4. Extract Year from Date
Pandas:

python

df['date_col'].dt.year
SQL:

sql

SELECT YEAR(date_col) FROM table;
Explanation:

Both Pandas and SQL have direct methods to extract the year from a date column.
5. Extract Month from Date
Pandas:

python

df['date_col'].dt.month
SQL:

sql

SELECT MONTH(date_col) FROM table;
Explanation:

Both Pandas and SQL provide functions to extract the month from a date column.
6. Extract Day from Date
Pandas:

python

df['date_col'].dt.day
SQL:

sql

SELECT DAY(date_col) FROM table;
Explanation:

Similarly, the day can be extracted from a date column using Pandas or SQL functions.
7. Filter Rows Based on String Matching
Pandas:

python

df[df['col1'].str.contains('pattern')]
SQL:

sql

SELECT * FROM table WHERE col1 LIKE '%pattern%';
Explanation:

In Pandas, str.contains('pattern') filters rows where col1 contains the pattern.
In SQL, LIKE '%pattern%' filters rows with a similar logic.
8. Aggregate Functions with Group By
Pandas:

python

df.groupby('col1')['col2'].mean()
SQL:

sql

SELECT col1, AVG(col2) FROM table GROUP BY col1;
Explanation:

Both Pandas and SQL allow aggregation of data, like calculating the average, using the groupby or GROUP BY clause.
9. Pivot Table
Pandas:

python

df.pivot_table(values='col1', index='col2', columns='val1')
SQL:

sql

SELECT * FROM table PIVOT (SUM(col1) FOR col2 IN (val1, val2));
Explanation:

Pandas provides pivot_table for reshaping data, while SQL uses PIVOT.
10. Unpivot Table
Pandas:

python

df.melt(id_vars=['id'], value_vars=['col1', 'col2'])
SQL:

sql

SELECT col1, col2 FROM table UNPIVOT (col FOR val IN (col1, col2));
Explanation:

melt in Pandas is equivalent to SQL’s UNPIVOT to convert wide-format data to long-format.
11. Calculate Cumulative Sum
Pandas:

python

df['col2'].cumsum()
SQL:

sql

SELECT col1, SUM(col2) OVER (ORDER BY col1) FROM table;
Explanation:

cumsum() in Pandas and SUM() OVER in SQL calculate the cumulative sum.
12. Calculate Moving Average
Pandas:

python

df['col2'].rolling(window=3).mean()
SQL:

sql

SELECT col1, AVG(col2) OVER (ORDER BY col1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM table;
Explanation:

Moving averages can be computed in both Pandas and SQL by specifying a window of rows.
13. Find the Row with Maximum Value in a Column
Pandas:

python

df.loc[df['col1'].idxmax()]
SQL:

sql

SELECT * FROM table ORDER BY col1 DESC LIMIT 1;
Explanation:

Pandas uses idxmax() and SQL uses ORDER BY DESC to find the row with the maximum value.
14. Find the Row with Minimum Value in a Column
Pandas:

python

df.loc[df['col1'].idxmin()]
SQL:

sql

SELECT * FROM table ORDER BY col1 ASC LIMIT 1;
Explanation:

Similarly, idxmin() in Pandas and ORDER BY ASC in SQL find the row with the minimum value.
15. Drop Duplicate Rows
Pandas:

python

df.drop_duplicates()
SQL:

sql

DELETE FROM table WHERE rowid NOT IN (SELECT MIN(rowid) FROM table GROUP BY col1, col2);
Explanation:

Pandas uses drop_duplicates() to remove duplicate rows, while SQL requires a subquery.
16. Calculate the Length of a String
Pandas:

python

df['col1'].str.len()
SQL:

sql

SELECT LENGTH(col1) FROM table;
Explanation:

Both Pandas and SQL have functions to calculate the length of strings in a column.
17. Convert String to Uppercase
Pandas:

python

df['col1'].str.upper()
SQL:

sql

SELECT UPPER(col1) FROM table;
Explanation:

upper() in Pandas and UPPER() in SQL convert strings to uppercase.
18. Convert String to Lowercase
Pandas:

python

df['col1'].str.lower()
SQL:

sql

SELECT LOWER(col1) FROM table;
Explanation:

Both Pandas and SQL have equivalent functions for converting strings to lowercase.
19. Strip Whitespace from String
Pandas:

python

df['col1'].str.strip()
SQL:

sql

SELECT TRIM(col1) FROM table;
Explanation:

strip() in Pandas and TRIM() in SQL remove leading and trailing spaces from strings.
20. Replace a Substring
Pandas:

python

df['col1'].str.replace('old', 'new')
SQL:

sql

SELECT REPLACE(col1, 'old', 'new') FROM table;
Explanation:

replace() in Pandas and REPLACE() in SQL are used to replace substrings within a string.
21. Check for Null Values
Pandas:

python

df[df['col1'].isnull()]
SQL:

sql

SELECT * FROM table WHERE col1 IS NULL;
Explanation:

Both Pandas and SQL can check for null values in a column.
22. Check for Non-null Values
Pandas:

python

df[df['col1'].notnull()]
SQL:

sql

SELECT * FROM table WHERE col1 IS NOT NULL;
Explanation:

Similarly, both Pandas and SQL check for non-null values in a column.
23. Conditional Column Based on Other Columns
Pandas:

python

df['col3'] = df['col2'].apply(lambda x: 'High' if x > 10 else 'Low')
SQL:

sql

SELECT col1, CASE WHEN col2 > 10 THEN 'High' ELSE 'Low' END AS col3 FROM table;
Explanation:

apply() in Pandas and CASE WHEN in SQL create conditional columns.
24. Drop Rows with Null Values
Pandas:

python

df.dropna()
SQL:

sql

DELETE FROM table WHERE col1 IS NULL;
Explanation:

Both Pandas and SQL can be used to remove rows with null values.
25. Create New Column from Existing Columns
Pandas:

python

df['col3'] = df['col1'] + df['col2']
SQL:

sql

SELECT col1, col2, (col1 + col2) AS col3 FROM table;
Explanation:

In both Pandas and SQL, you can create new columns by combining existing ones.
26. Calculate Percentage of a Column
Pandas:

python

df['percentage'] = df['col1'] / df['col1'].sum() * 100
SQL:

sql

SELECT col1, (col1 / SUM(col1) OVER()) * 100 AS percentage FROM table;
Explanation:

Both Pandas and SQL allow you to calculate the percentage contribution of each row.
27. Insert a New Row
Pandas:

python

df.loc[len(df)] = [val1, val2]
SQL:

sql

INSERT INTO table (col1, col2) VALUES (val1, val2);
Explanation:

Inserting a new row can be done in Pandas with loc[] or using INSERT INTO in SQL.
28. Update a Row
Pandas:

python

df.loc[df['col2'] == val2, 'col1'] = val1
SQL:

sql

UPDATE table SET col1 = val1 WHERE col2 = val2;
Explanation:

Updating rows in Pandas uses loc[], whereas SQL uses UPDATE SET.
vbnet


You can save this content as a `.md` file using any text editor. Let me know if you need further assistance!
You said:
続きをお願いします
ChatGPT said:
ChatGPT
Certainly! Here is the continuation:

markdown

## 29. Add a Constant Value to a Column

**Pandas:**

```python
df['col1'] = df['col1'] + 10
SQL:

sql

SELECT col1 + 10 FROM table;
Explanation:

In Pandas, you can add a constant value to each element of a column using the + operator. Similarly, in SQL, you can add a constant value to the column in the SELECT statement.
30. Multiply Two Columns
Pandas:

python

df['col3'] = df['col1'] * df['col2']
SQL:

sql

SELECT col1 * col2 AS col3 FROM table;
Explanation:

Multiplying two columns together to create a new column is straightforward in both Pandas and SQL.
31. Divide Two Columns
Pandas:

python

df['col3'] = df['col1'] / df['col2']
SQL:

sql

SELECT col1 / col2 AS col3 FROM table;
Explanation:

You can divide the values of two columns to produce a new column in both Pandas and SQL.
32. Apply a Custom Function to a Column
Pandas:

python

df['col2'] = df['col1'].apply(lambda x: x + 2)
SQL:

sql

SELECT col1 + 2 AS col2 FROM table;
Explanation:

In Pandas, apply() allows you to apply a custom function to each element in a column. In SQL, this can be done directly in the SELECT statement.
33. Filter Rows Based on a Condition
Pandas:

python

df[df['col1'] > 5]
SQL:

sql

SELECT * FROM table WHERE col1 > 5;
Explanation:

Filtering rows based on a condition is straightforward in both Pandas and SQL.
34. Sort Values
Pandas:

python

df.sort_values(by='col1', ascending=False)
SQL:

sql

SELECT * FROM table ORDER BY col1 DESC;
Explanation:

Both Pandas and SQL have built-in functionality for sorting data by one or more columns.
35. Remove Columns
Pandas:

python

df.drop(columns=['col1'])
SQL:

sql

ALTER TABLE table DROP COLUMN col1;
Explanation:

In Pandas, you can remove a column using drop(columns=[]), while in SQL, the ALTER TABLE statement is used to drop a column.
36. Rename Columns
Pandas:

python

df.rename(columns={'old_col_name': 'new_col_name'})
SQL:

sql

ALTER TABLE table RENAME COLUMN old_col_name TO new_col_name;
Explanation:

Renaming columns in Pandas uses the rename() method, whereas SQL uses the ALTER TABLE RENAME command.
37. Reset Index
Pandas:

python

df.reset_index(drop=True)
SQL:

sql

ALTER TABLE table AUTO_INCREMENT = 1;
Explanation:

Resetting the index in Pandas can be done with reset_index(). In SQL, you can reset the AUTO_INCREMENT value for the primary key index.
38. Set a Column as Index
Pandas:

python

df.set_index('col1')
SQL:

sql

ALTER TABLE table ADD PRIMARY KEY (col1);
Explanation:

In Pandas, set_index() is used to set a column as the index. In SQL, you can achieve this by setting a column as the PRIMARY KEY.
39. Merge DataFrames (Inner Join)
Pandas:

python

pd.merge(df1, df2, on='col1')
SQL:

sql

SELECT * FROM table1 INNER JOIN table2 ON table1.col1 = table2.col1;
Explanation:

Both Pandas and SQL support merging two datasets based on a common key.
40. Merge DataFrames (Left Join)
Pandas:

python

pd.merge(df1, df2, on='col1', how='left')
SQL:

sql

SELECT * FROM table1 LEFT JOIN table2 ON table1.col1 = table2.col1;
Explanation:

A left join is achieved in Pandas using merge() with how='left' and in SQL with LEFT JOIN.
41. Merge DataFrames (Right Join)
Pandas:

python

pd.merge(df1, df2, on='col1', how='right')
SQL:

sql

SELECT * FROM table1 RIGHT JOIN table2 ON table1.col1 = table2.col1;
Explanation:

A right join can be performed in Pandas using merge() with how='right' and in SQL with RIGHT JOIN.
42. Merge DataFrames (Outer Join)
Pandas:

python

pd.merge(df1, df2, on='col1', how='outer')
SQL:

sql

SELECT * FROM table1 FULL OUTER JOIN table2 ON table1.col1 = table2.col1;
Explanation:

An outer join returns all records when there is a match in either left or right table. This can be done in Pandas using merge() with how='outer' and in SQL with FULL OUTER JOIN.
43. Filter Data Based on Multiple Conditions
Pandas:

python

df[(df['col1'] > 5) & (df['col2'] < 10)]
SQL:

sql

SELECT * FROM table WHERE col1 > 5 AND col2 < 10;
Explanation:

Filtering with multiple conditions in Pandas uses & for AND logic, while SQL uses AND.
44. Drop Rows Based on Condition
Pandas:

python

df.drop(df[df['col1'] > 5].index)
SQL:

sql

DELETE FROM table WHERE col1 > 5;
Explanation:

In Pandas, you can drop rows based on a condition using drop(). In SQL, the DELETE statement removes rows based on the condition.
45. Change Data Type of a Column
Pandas:

python

df['col1'] = df['col1'].astype(int)
SQL:

sql

ALTER TABLE table MODIFY col1 INT;
Explanation:

You can change the data type of a column in Pandas with astype(), while SQL uses ALTER TABLE MODIFY for this purpose.
46. Apply Conditional Formatting
Pandas:

python

df.style.applymap(lambda x: 'color: red' if x < 0 else 'color: black')
SQL:

sql

-- SQL doesn't support conditional formatting directly in query results; it is often handled in the presentation layer.
Explanation:

Conditional formatting can be applied in Pandas using the style attribute. In SQL, formatting is usually handled outside the query in the presentation layer.
47. Create a Categorical Variable
Pandas:

python

df['col1'] = pd.Categorical(df['col1'], categories=['Low', 'Medium', 'High'])
SQL:

sql

ALTER TABLE table ADD COLUMN col1 ENUM('Low', 'Medium', 'High');
Explanation:

In Pandas, categorical variables are created using pd.Categorical(). In SQL, ENUM can be used to define categorical values in a column.


48. Handle Missing Data with Interpolation

Pandas:
```python
df['col1'].interpolate()
```

SQL:
```sql
-- SQL doesn't support interpolation directly. It's usually done in the application layer or with advanced analytics functions.
```

Explanation:

Pandas allows for handling missing data with interpolation using interpolate(). In SQL, this often requires additional logic or is handled outside of SQL.
javascript


You can save this continuation as part of the `.md` file along with the previous content provided. If you need the full file or further assistance, feel free to ask!
You said:

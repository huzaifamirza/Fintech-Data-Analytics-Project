use data_bank;

# A. Customer Nodes Exploration
# Q1 How many unique nodes are there on the Data Bank system?
SELECT COUNT(DISTINCT node_id) AS Unique_nodes FROM customer_nodes;

# Q2 What is the number of nodes per region?
SELECT c.region_id, region_name, COUNT(node_id) as nodes_per_region FROM customer_nodes c INNER JOIN regions r ON r.region_id = c.region_id GROUP BY 1, 2 ORDER BY 1;

# Q3 How many customers are allocated to each region?
SELECT c.region_id, region_name, COUNT(DISTINCT customer_id) as customer_per_region FROM customer_nodes c INNER JOIN regions r ON r.region_id = c.region_id GROUP BY 1, 2 ORDER BY 1;

# Q4 How many days on average are customers reallocated to a different node?
SELECT AVG(datediff(end_date,start_date)) as avg_reallocated_days FROM customer_nodes WHERE end_date != '9999-12-31';

# Q5 What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
-- WITH date_diff AS
-- (
-- 	SELECT cn.customer_id,
-- 	       cn.region_id,
-- 	       r.region_name,
-- 	       DATEDIFF(DAY, start_date, end_date) AS reallocation_days
-- 	FROM customer_nodes cn
-- 	INNER JOIN regions r
-- 	ON cn.region_id = r.region_id
-- 	WHERE end_date != '9999-12-31'
-- )

-- SELECT DISTINCT region_id,
-- 	        region_name,
-- 	        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY reallocation_days) OVER(PARTITION BY region_name) AS median,
-- 	        PERCENTILE_CONT(0.8) WITHIN GROUP(ORDER BY reallocation_days) OVER(PARTITION BY region_name) AS percentile_80,
-- 	        PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY reallocation_days) OVER(PARTITION BY region_name) AS percentile_95
-- FROM date_diff
-- ORDER BY region_name;

# B. Customer Transactions
# Q6 What is the unique count and total amount for each transaction type?
SELECT txn_type, COUNT(*) AS Unique_Count, SUM(txn_amount) AS Total_Amt FROM customer_transactions GROUP BY 1;

# Q7 What is the average total historical deposit counts and amounts for all customers?
SELECT AVG(Unique_Count) AS Cnts, AVG(Total_Amt) FROM (SELECT customer_id, COUNT(*) AS Unique_Count, SUM(txn_amount) AS Total_Amt FROM customer_transactions WHERE txn_type = 'deposit' GROUP BY 1) AS e;

# Q8 For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
WITH cte AS (SELECT monthname(txn_date) as name,month(txn_date) as mon,customer_id, COUNT(case when txn_type = 'deposit' then 1 end) as deposit,
COUNT(case when txn_type = 'purchase' then 1 end) as purchase,
 COUNT(case when txn_type = 'withdrawal' then 1 end) as withdrawal
FROM customer_transactions GROUP BY 1,2,3 ORDER BY 2)
SELECT name, COUNT(DISTINCT customer_id) AS counts FROM cte WHERE deposit > 1 AND (purchase > 0 OR withdrawal > 0) GROUP BY 1;

# Q9 What is the closing balance for each customer at the end of the month?
WITH cte AS (
SELECT customer_id, monthname(txn_date) AS name, MAX(txn_date) as max_date FROM customer_transactions GROUP BY 1,2
)
SELECT t.customer_id, monthname(t.txn_date) as mm, c.max_date, SUM(Case when txn_type = 'deposit' then txn_amount else -1 * txn_amount end) AS closing_balance
from customer_transactions t join cte c 
on t.customer_id = c.customer_id and c.name = monthname(t.txn_date) and t.txn_date = c.max_date
GROUP BY 1,2,3 Order by 1 ;

# Q10 What is the percentage of customers who increase their closing balance by more than 5%?
#CTE 1: Monthly transactions of each customer
WITH monthly_transactions AS
(
	SELECT customer_id,
	       EOMONTH(txn_date) AS end_date,
	       SUM(CASE WHEN txn_type IN ('withdrawal', 'purchase') THEN -txn_amount
					ELSE txn_amount END) AS transactions
	FROM customer_transactions
	GROUP BY customer_id, EOMONTH(txn_date)
),

-- CTE 2: Claculate the closing balance for each customer for each month
closing_balances AS 
(
	SELECT customer_id,
	       end_date,
	       COALESCE(SUM(transactions) OVER(PARTITION BY customer_id ORDER BY end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS closing_balance
	FROM monthly_transactions
),

-- CTE3: Calculate the percentage increase in closing balance for each customer for each month
pct_increase AS 
(
  SELECT customer_id,
	 end_date,
	 closing_balance,
	 LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date) AS prev_closing_balance,
         100 * (closing_balance - LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date)) / NULLIF(LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date), 0) AS pct_increase
 FROM closing_balances
)

-- Calculate the percentage of customers whose closing balance increased 5% compared to the previous month
SELECT CAST(100.0 * COUNT(DISTINCT customer_id) / (SELECT COUNT(DISTINCT customer_id) FROM customer_transactions) AS FLOAT) AS pct_customers
FROM pct_increase
WHERE pct_increase > 5;



# C. Data Allocation Challenge
# running customer balance column that includes the impact each transaction.
SELECT customer_id, txn_date, txn_type, txn_amount, Sum(Case when txn_type = 'deposit' then txn_amount
when txn_type = 'withdrawal' then -txn_amount
when txn_type = 'purchase' then -txn_amount
else 0 end
) over(partition by customer_id order by txn_date) as running_total from customer_transactions;

# customer balance at the end of each month
WITH cte AS (
SELECT customer_id, monthname(txn_date) AS name, MAX(txn_date) as max_date FROM customer_transactions GROUP BY 1,2
)
SELECT t.customer_id, monthname(t.txn_date) as mm, c.max_date, SUM(Case when txn_type = 'deposit' then txn_amount else -1 * txn_amount end) AS closing_balance
from customer_transactions t join cte c 
on t.customer_id = c.customer_id and c.name = monthname(t.txn_date) and t.txn_date = c.max_date
GROUP BY 1,2,3 Order by 1 ;

# minimum, average and maximum values of the running balance for each customer. 
SELECT customer_id, MIN(running_total) as Min_running_total, MAX(running_total) AS Max_running_total, round(AVG(running_total),2) AS Avg_running_total FROM (
SELECT customer_id, txn_date, txn_type, txn_amount, Sum(Case when txn_type = 'deposit' then txn_amount
when txn_type = 'withdrawal' then -txn_amount
when txn_type = 'purchase' then -txn_amount
else 0 end
) over(partition by customer_id order by txn_date) as running_total from customer_transactions) e 
group by 1
;

# For option 1: data is allocated based off the amount of money at the end of the previous month.  
with transaction_amt_cte as (
Select customer_id, txn_date, txn_type, monthname(txn_date) as names, SUM(Case when txn_type = 'deposit' then txn_amount else -1 * txn_amount end) AS balance
from customer_transactions
group by 1,2,3,4
),
running_cust_balance as (
Select customer_id, txn_date, names, balance, Sum(Case when txn_type = 'deposit' then balance
when txn_type = 'withdrawal' then -balance
when txn_type = 'purchase' then -balance
else 0 end
) over(partition by customer_id,names order by txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_cus_total from transaction_amt_cte
),
cust_end_month as (
Select customer_id,names, max(running_cus_total) as end_mon_bal from running_cust_balance group by 1,2
)
select names, sum(end_mon_bal) as end_month_bal from  cust_end_month group by 1 order by 2 desc;

# For Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days. 
with transaction_amt_cte as (
Select customer_id, monthname(txn_date) as names, SUM(Case when txn_type = 'deposit' then txn_amount else -1 * txn_amount end) AS balance
from customer_transactions
group by 1,2
),
running_cust_balance as (
Select customer_id, names,balance, sum(balance) over(partition by customer_id order by names) as running_bal
from transaction_amt_cte
),
avg_running_cust_balance as (
select customer_id, round((running_bal),2) as avg_bal from running_cust_balance group by 1
)
Select names, -1 * sum(avg_bal) as data_req from avg_running_cust_balance a 
join running_cust_balance b 
on b.customer_id = a.customer_id
group by 1 
order by 2;

# For option 3: data is updated real-time.
with transaction_amt_cte as (
Select customer_id, monthname(txn_date) as names,txn_date,txn_type,txn_amount, SUM(Case when txn_type = 'deposit' then txn_amount else -1 * txn_amount end) AS balance
from customer_transactions
group by 1,2,3,4,5
),
running_cust_balance as (
Select customer_id, names,balance, sum(balance) over(partition by customer_id order by names) as running_bal
from transaction_amt_cte
)
Select names, sum(running_bal) as data_req from running_cust_balance group by 1;



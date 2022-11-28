import os
import pandas as pd
import numpy as np
from datetime import date
from pandasql import sqldf

n_rows_total = 20000
start_date = pd.to_datetime('2019-01-01')
end_date = date.today()
ExportPath = os.getcwd() + "\statistic_view.xlsx"
print(ExportPath)
all_users_ids = np.arange(1, 2001)
all_users_ages = np.arange(18, 56)
all_purchases_ids = np.arange(1, n_rows_total + 1)
all_items_ids = np.arange(1, 1001)
all_items = ['item' + str(item) for item in all_items_ids]
all_prices = np.arange(400, 5000, 0.25, dtype=float)
all_dates = pd.date_range(start_date, end=end_date, freq='D')

user_ids = np.random.choice(all_users_ids, n_rows_total)
users_ages = np.random.choice(all_users_ages, n_rows_total)
# all_purchases_ids
dates = np.random.choice(all_dates, n_rows_total)
items = np.random.choice(all_items, n_rows_total)
prices = np.random.choice(all_prices, n_rows_total)

statistic_view = pd.DataFrame({
    'userid': user_ids,
    'age': users_ages,
    'purchaseid': all_purchases_ids,
    'date': dates,
    'itemid': items,
    'price': prices
})
print("Экспорт сгенерированных данных в ексель (в папку с проектом)")
statistic_view.to_excel(r"" + ExportPath)

max_avg_by_month_user_age_before_35 = """SELECT
distinct strftime('%m',date) As "month",
AVG(price) over (partition by strftime('%m',date)) as "max_AVG_by_Month"
FROM statistic_view
where age >=35
order by AVG(price) over (partition by strftime('%m',date)) Desc
Limit 1"""
AVG_price_by_month_18_25 = """SELECT
(SUM(price)/((cast(julianday(MAX(date)) - julianday(MIN(date))as Integer))/30))/ COUNT(DISTINCT userid) as "AVG_BY_MONTH"
from statistic_view
where age >= 18 and age <=25"""
AVG_price_by_month_26_35 = """SELECT
(SUM(price)/((cast(julianday(MAX(date)) - julianday(MIN(date))as Integer))/30))/ COUNT(DISTINCT userid) as "AVG_BY_MONTH"
from statistic_view
where age >= 26 and age <=35"""
max_purchases_item_by_current_year = """SELECT itemid
From
(SELECT distinct itemid,
Sum(price) over (partition by itemid) As "sum_price"
FROM statistic_view
where strftime('%Y',date) = strftime('%Y',date('now'))) as "subselect"
Order by sum_price Desc Limit 1"""
top_3_item_by_all_period = """Select itemid, sum_price,
sum_price/sum(sum_price) OVER() AS "Ratio"
FROM
(SELECT distinct itemid,
sum(price) over (partition by itemid) As "sum_price"
FROM statistic_view) As "subselect"
group by itemid,sum_price
Order by sum_price Desc Limit 3"""

print("Сколько в среднем в месяц тратят пользователи от 18 до 25 лет включительно :")
print(sqldf(AVG_price_by_month_18_25))

print("Сколько в среднем в месяц тратят пользователи от 26 до 35 лет включительно :")
print(sqldf(AVG_price_by_month_26_35))

print("В каком месяце года максимальная средняя прибыль от пользователей 35+ :")
print(sqldf(max_avg_by_month_user_age_before_35))

print("Товар который принес максимальную прибыль в текущем году :")
print(sqldf(max_purchases_item_by_current_year))

print("Топ-3 товара за весь период и их доля в прибыли :")
print(sqldf(top_3_item_by_all_period))

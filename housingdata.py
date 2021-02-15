import pandas as pd
import mysql.connector

db = mysql.connector.connect(
    host="127.0.0.1",               #hostname
    user="root",                   # the user who has privilege to the db
    passwd="bloodyguysr1!",               #password for user
    database="housing_data",               #database name
    auth_plugin = 'mysql_native_password',
)

cursor = db.cursor()
#CREATE TABLE, only do this ONCE
# cursor.execute("""
#     CREATE TABLE IF NOT EXISTS ZHVI(
#         regionId INT,
#         sizeRank INT,
#         date DATE,
#         city varchar(100),
#         zhviValue DOUBLE
#     )
# """)

#DROP TABLE, only do when necessary
# cursor.execute("""
#     DROP TABLE ZHVI
# """)

df = pd.read_csv('metro_zhvi.csv')
colNames = df.columns[5:]

#INSERT INTO TABLE, only do this ONCE
# for row in df.itertuples():
#     regionId, sizeRank, city, = row[1], row[2], row[3]
#     for colVal, colName in zip(row[6:], colNames):
#         date = colName.zfill(10)
#         date = date[-4:] + '-' + date[:2] + '-' + date[-7:-5]
#         query = ("""
#             INSERT INTO ZHVI
#             (regionId, sizeRank, date, city, zhviValue)
#             VALUES (%s, %s, %s, %s, %s)
#         """)
#         tupleData = (regionId, sizeRank, date, city, colVal)
#         cursor.execute(query, tupleData)
#         db.commit()

# number of total cities/metros
cursor.execute("""
    select count(*)
    from
        (select count(*)
        from zhvi
        group by regionId) a
""")
totalCities = cursor.fetchone() # 911

# number of entries per metro. starts 9/30/17 - 12/31/20
cursor.execute("""
    select count(*)
    from zhvi
    group by regionId
    LIMIT 1
""")
entriesPerCity = cursor.fetchone() # 40

# top 20 metros by avg ZHVI
cursor.execute("""
    select city, FORMAT(avg(zhviValue),0) as avgZHVI
    from zhvi
    group by regionId
    order by avg(zhviValue) desc
    LIMIT 20
""")
top20Cities = cursor.fetchall()

# top 20 states by avg ZHVI
cursor.execute("""
    SELECT RIGHT(city, 2) AS state, format(avg(zhviValue),0) as avgZHVI
    from zhvi
    where city != 'United States'
    group by state
    order by avg(zhviValue) desc
    limit 20
""")
top20States = cursor.fetchall()

# most ZHVI increase in the past 4 years by city/metro (percentage, 9/2017 - 12/2020)
cursor.execute("""
    SELECT z1.city, (z1.zhviValue - z2.zhviValue)*100.0 / z2.zhviValue as percentChange
    from zhvi z1
    join zhvi z2 using(regionId)
    group by z1.city
    order by percentChange desc
    LIMIT 20
""")
mostZHVIincrease = cursor.fetchall()

# top 5 most ZHVI increase by year (2018,2019,2020)
cursor.execute("""
    select city,
        (y2018 - y2017)*100.0 / y2017 as percentChange2018,
        (y2019 - y2018)*100.0 / y2018 as percentChange2019,
        (y2020 - y2019)*100.0 / y2019 as percentChange2020
    FROM
        (Select city,
            sum(case when y2017 is not null then y2017 end) as y2017,
            sum(case when y2018 is not null then y2018 end) as y2018,
            sum(case when y2019 is not null then y2019 end) as y2019,
            sum(case when y2020 is not null then y2020 end) as y2020
        FROM
            (SELECT city,
                case when year=2017 then avgZHVI end as y2017,
                case when year=2018 then avgZHVI end as y2018,
                case when year=2019 then avgZHVI end as y2019,
                case when year=2020 then avgZHVI end as y2020
            FROM
                (select extract(year from date) as year, city, avg(zhviValue) as avgZHVI
                from zhvi
                where city != 'United States'
                group by year, city) a) b
        group by city) c
    order by percentChange2020 desc
    LIMIT 20
""")
zhviIncreaseByYear = cursor.fetchall()
print(zhviIncreaseByYear)

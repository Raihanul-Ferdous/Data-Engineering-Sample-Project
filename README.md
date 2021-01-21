
Global Temprature and Population
Data Engineering Capstone Project
Project Summary
This project discusses how has temprature and population changed over months

The project follows the follow steps:

Step 1: Scope the Project and Gather Data
Step 2: Explore and Assess the Data
Step 3: Define the Data Model
Step 4: Run ETL to Model the Data
Step 5: Complete Project Write Up
In [1]:
# Imports
import pandas as pd
Step 1: Scope the Project and Gather Data
Scope
Discussing temprature changes and population was the initial scope of the project. But was not able to find population of different cities over months so plugged in a population of different cities of an year and have discussed how this might change if such a dataset is present.

We'll be using Amazon Redshift as our warehouse tool for storing our datasets from which we arrive at a star schema. This can be extended to use Apache Airflow to automate pipelines for streaming data.

About Data
Temperature Data found in https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data#GlobalLandTemperaturesByCity.csv

For Population https://www.kaggle.com/max-mind/world-cities-database#worldcitiespop.csv

Population has country code but not country name so dataset that solves this by merging is found in : https://pkgstore.datahub.io/core/country-list/data_json/data/8c458f2d15d9f2119654b29ede6e45b8/data_json.json

Step 2: Explore and Assess the Data
Exploring the Data
On Exploring the dataset it looks clean and missing fields are because it doesn't contain value and can't be subjected to outlier treatment.

In [3]:
# Temprature Data
df = pd.read_csv('../../data2/GlobalLandTemperaturesByCity.csv')
df.head()
Out[3]:
dt	AverageTemperature	AverageTemperatureUncertainty	City	Country	Latitude	Longitude
0	1743-11-01	6.068	1.737	Århus	Denmark	57.05N	10.33E
1	1743-12-01	NaN	NaN	Århus	Denmark	57.05N	10.33E
2	1744-01-01	NaN	NaN	Århus	Denmark	57.05N	10.33E
3	1744-02-01	NaN	NaN	Århus	Denmark	57.05N	10.33E
4	1744-03-01	NaN	NaN	Århus	Denmark	57.05N	10.33E
In [4]:
# Population Data
pop_df = pd.read_csv('data/worldcitiespop.csv')
pop_df.head()
/opt/conda/lib/python3.6/site-packages/IPython/core/interactiveshell.py:2785: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
  interactivity=interactivity, compiler=compiler, result=result)
Out[4]:
Country	City	AccentCity	Region	Population	Latitude	Longitude
0	ad	aixas	Aixàs	6	NaN	42.483333	1.466667
1	ad	aixirivali	Aixirivali	6	NaN	42.466667	1.500000
2	ad	aixirivall	Aixirivall	6	NaN	42.466667	1.500000
3	ad	aixirvall	Aixirvall	6	NaN	42.466667	1.500000
4	ad	aixovall	Aixovall	6	NaN	42.466667	1.483333
Step 3: Define the Data Model
3.1 Conceptual Data Model
We look into star schema as two datasets has it's own prime focus namely temperature and population. Star schema is preferred because the temprature and population are measures of central tendency and are aggreagated over date and city. Thus placing the measures in fact table and aggregation attributes on dimension tables linked with foreign keys provides query optimization, easy to understand and easy to update model.

Dimension Tables:
Time Table : Date, Day, Month, Year
City Table : City_key,Code,City,Latitude,Longitude,Country
Fact Tables:
Main Table : Date,City_key,Temperature,Population
3.2 Mapping Out Data Pipelines
Steps for mapping are discussed below :

Temprature Data
In [7]:
# Creating city_df from GlobalLandTemperaturesByCity.csv
city_df = df[['City','Country']]
city_df = city_df.drop_duplicates()
# Creating city_key from city_df
city_df['city_key'] = city_df.reset_index().index
city_df.head()
Out[7]:
City	Country	city_key
0	Århus	Denmark	0
3239	Çorlu	Turkey	1
6478	Çorum	Turkey	2
9607	Öskemen	Kazakhstan	3
11925	Ürümqi	China	4
In [8]:
# merge city key with temp data to fill city_key
temp_data = pd.merge(df,city_df,how='left',on='City')
temp_data.head()
Out[8]:
dt	AverageTemperature	AverageTemperatureUncertainty	City	Country_x	Latitude	Longitude	Country_y	city_key
0	1743-11-01	6.068	1.737	Århus	Denmark	57.05N	10.33E	Denmark	0
1	1743-12-01	NaN	NaN	Århus	Denmark	57.05N	10.33E	Denmark	0
2	1744-01-01	NaN	NaN	Århus	Denmark	57.05N	10.33E	Denmark	0
3	1744-02-01	NaN	NaN	Århus	Denmark	57.05N	10.33E	Denmark	0
4	1744-03-01	NaN	NaN	Århus	Denmark	57.05N	10.33E	Denmark	0
In [9]:
# Selecting required data from temp_data
temp_data = temp_data[['dt','AverageTemperature','AverageTemperatureUncertainty','City','Country_x','Latitude','Longitude','city_key']]
temp_data = temp_data.rename(columns={"Country_x":"Country"})
temp_data.head()
Out[9]:
dt	AverageTemperature	AverageTemperatureUncertainty	City	Country	Latitude	Longitude	city_key
0	1743-11-01	6.068	1.737	Århus	Denmark	57.05N	10.33E	0
1	1743-12-01	NaN	NaN	Århus	Denmark	57.05N	10.33E	0
2	1744-01-01	NaN	NaN	Århus	Denmark	57.05N	10.33E	0
3	1744-02-01	NaN	NaN	Århus	Denmark	57.05N	10.33E	0
4	1744-03-01	NaN	NaN	Århus	Denmark	57.05N	10.33E	0
City Codes
In [10]:
# Reading city_codes.json
city_codes = pd.read_json('data/city_codes.json')
city_codes.head()
Out[10]:
Code	Name
0	AF	Afghanistan
1	AX	Åland Islands
2	AL	Albania
3	DZ	Algeria
4	AS	American Samoa
In [11]:
# Transformation : Since the code in pop_data has to match with city_code they're converted to lowercase and Column Name as Country 
city_codes['Code'] = city_codes['Code'].str.lower()
city_codes = city_codes.rename(columns={"Name":"Country"})
city_codes.head()
Out[11]:
Code	Country
0	af	Afghanistan
1	ax	Åland Islands
2	al	Albania
3	dz	Algeria
4	as	American Samoa
Population Dataset
In [12]:
# Selecting required columns from pop_df
pop_df = pop_df[['Country','AccentCity','Population','Latitude','Longitude']]
pop_df.head()
Out[12]:
Country	AccentCity	Population	Latitude	Longitude
0	ad	Aixàs	NaN	42.483333	1.466667
1	ad	Aixirivali	NaN	42.466667	1.500000
2	ad	Aixirivall	NaN	42.466667	1.500000
3	ad	Aixirvall	NaN	42.466667	1.500000
4	ad	Aixovall	NaN	42.466667	1.483333
In [13]:
# Transformation
pop_df = pop_df.loc[pop_df['Population'] > 0]
pop_df = pop_df.drop_duplicates()
pop_df = pop_df.rename(columns={"Country":"Code","AccentCity":"City"})
pop_df.head()
Out[13]:
Code	City	Population	Latitude	Longitude
6	ad	Andorra la Vella	20430.0	42.500000	1.516667
20	ad	Canillo	3292.0	42.566667	1.600000
32	ad	Encamp	11224.0	42.533333	1.583333
49	ad	La Massana	7211.0	42.550000	1.516667
53	ad	Les Escaldes	15854.0	42.500000	1.533333
In [14]:
# Merging with city_codes to get Country
pop_data = pd.merge(pop_df,city_codes,how='inner',on='Code')
pop_data = pd.merge(pop_data,city_df,how='inner',on=['City','Country'])
pop_data.head()
Out[14]:
Code	City	Population	Latitude	Longitude	Country	city_key
0	ae	Abu Dhabi	603687.0	24.466667	54.366667	United Arab Emirates	22
1	ae	Dubai	1137376.0	25.258172	55.304717	United Arab Emirates	839
2	ae	Sharjah	543942.0	25.357310	55.403304	United Arab Emirates	2819
3	af	Baglan	108481.0	36.130684	68.708286	Afghanistan	216
4	af	Gardez	103732.0	33.597439	69.225922	Afghanistan	1000
Dataset Quantity
In [15]:
# Temperature Dataset
temp_data.count()
Out[15]:
dt                               8828359
AverageTemperature               8457500
AverageTemperatureUncertainty    8457500
City                             8828359
Country                          8828359
Latitude                         8828359
Longitude                        8828359
city_key                         8828359
dtype: int64
In [16]:
# Population Dataset
pop_data.count()
Out[16]:
Code          3359
City          3359
Population    3359
Latitude      3359
Longitude     3359
Country       3359
city_key      3359
dtype: int64
Writing out Datasets to two different formats
In [17]:
# Population Dataset is converted to json and to a format where JSON File Path can be used to Load Data into redshift
out = pop_data.to_json(orient='records')[1:-1].replace('},{', '} {')
with open('pop_data.json', 'w') as f:
    f.write(out)
In [18]:
# Temperature Dataset is converted to CSV
temp_data.to_csv("temp_data.csv",index=False)
Push pop_data.json and temp_data.csv to S3
Step 4: Run Pipelines to Model the Data
4.1 Create the data model
Build the data pipelines to create the data model.

Create Table Code
In [1]:
# %load create_tables.py
import configparser
import psycopg2
from sql_queries import drop_table_queries,create_table_queries

def drop_tables(cur, conn):
    """
    Drops all table if already exists in sql_queries.py 
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    """
    Creates all tables in sql_queries.py 
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    1. Initialize a config parser to read configuration on dwh.cfg file
    2. Create connection and cursor for postgresql
    3. Loading functions
    4. Close connection
    """
    print("Please wait while executing...")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    print("Done !!")


if __name__ == "__main__":
    main()
ETL Code
In [2]:
# %load etl.py
import configparser
import psycopg2
from sql_queries import copy_table_queries,insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data into staging tables i.e,. pop_staging and temp_staging table via copy command in sql_queries.py
    """
    for query in copy_table_queries:
        print (query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Inserts data into appropriate analytical tables i.e,. songplays, users, songs, artists, time from respective staging tables in sql_queries.py
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    1. Initialize a config parser to read configuration on dwh.cfg file
    2. Create connection and cursor for postgresql
    3. Loading functions
    4. Close connection
    """
    print("Please wait while executing... may take upto 30 mins")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()
    print("Done !!")

if __name__ == "__main__":
    main()
4.2 Data Quality Checks
Run on Redshift Cluster
Value Counts

select count(*) from pop_staging;
select count(*) from temp_staging;
select count(*) from city_table;
select count(*) from time_table;
select count(*) from main_table;

Check Duplicates
These queries should provide single city_key for a specified city without duplicates

city_table

select city_key,city from city_table where city='Madras';
select city_key,city from city_table where city='Madurai';

time_table

select * from time_table where day=1 and month=3 and year=1800;
select * from time_table where day=1 and month=4 and year=1800;

4.3 Data dictionary
Dimension Tables:
Time Table : Date, Day, Month, Year
Date:
Type : Date
Description : Date at which the readings correspond to
Day:
Type : Int
Description : Day at which the readings correspond to
Month :
Type : Int
Description : Month at which the readings correspond to ###### Year:
Type : Int
Description : Year at which the readings correspond to
City Table : City_key,Code,City,Latitude,Longitude,Country
City_key:
Type : Int
Description : Unique Key that corresponds to each city
Code:
Type : Varchar
Description : Country Code for each city
City:
Type : Varchar
Description : City name
Latitude:
Type : Varchar
Description : Latitude of the city
Longitude:
Type : Varchar
Description : Longitude of the city
Country:
Type : Varchar
Description : Country Name
Fact Tables:
Main Table : Date,City_key,Temperature,Population
Date:
Type : Date
Description : Date at which the readings correspond to
City_key:
Type : Int
Description : Unique Key that corresponds to each city
Temperature:
Type : Flot
Description : Temperature for that city at that date
Population:
Type : Bigint
Description : Population for that city
Limitation : Since the population is overtime I've plugged in data with said year on dataset
Result Justification
The model has to have single entry for a city_key and date, Suppose we need population and temperature for the city Madras on 1800-12-01 which are primary keys on their dimension table can be applied below to retrieve data from fact table.


select * from public."main_table"
where city_key=1832 and date='1800-12-01'
limit 10;

To find temperature change over years below query is executed

select temperature from public."main_table"
where city_key=1832 and date between '1797-08-01' and '1812-08-01';
</code>
Step 5: Complete Project Write Up
Since this is batch dataset using a warehouse was sufficient thus Redshift was a choice if the dataset is continuously updated we might look into tool like Apache Airflow.
Since the dataset is over first day over months assuming it updates every month airflow can be scheduled to run pipeline on 2nd day of every month.
Scenarios:
The data was increased by 100x - Redshift can handle such speeds.
The data populates a dashboard that must be updated on a daily basis by 7am every day - Run a dashboard that pulls data from Redshift over said time
The database needed to be accessed by 100+ people - Cluster availability of redshift has to be optimized
Future Enhancement:
If a dataset for population over months for each city is found the insert statement on main table must be


insert into main_table (date,city_key,temperature,population) select t.dt,t.city_key,t.averagetemperature,p.population from temp_staging as t join pop_staging as p on t.city_key=p.city_key and t.dt=p.date;

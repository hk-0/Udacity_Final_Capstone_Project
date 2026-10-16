# Udacity Data Engineer Capstone Project

## Project Summary
The objective of this project was to create an ETL pipeline for i94 immigration, US city demographics, global land temperatures and US Airport datasets to form an analytical database related to immigration patterns. This database can then be used to look for different trends in immigration such as city temperature and its impact on immigrants, popular arrival airports for immigrants, or immigration to cities with diverse population.

The project follows the following steps:

- Step 1: Scope the Project and Gather Data
- Step 2: Explore and Assess the Data
- Step 3: Define the Data Model
- Step 4: Run ETL to Model the Data
- Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data

### Scope
The goal of this project is to pull the data from the four datasets listed below and create a set of fact and dimension tables for analysis of it.

### Describe and Gather Data
Datasets:

1. **I94 Immigration Data** : This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
2. **World Temperature Data** : This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
3. **U.S. City Demographic Data** : This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
4. **Airport Code Table** : This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).

Tools:
    
    AWS S3 for data storage
    Python for data processes
        - Pandas for analysis and processesing
        - Spark for processing
    

## Step 2: Explore and Assess the Data¶

Explore each dataset and clean the data as necessary

*See the Capstone Project.ipynb notebook for more details*

## Step 3: Define the Data Model

### 3.1 Conceptual Data Model

Star schema was chosed as the data model as it is simple and effective in terms of query performance. The data could be denormalized down to fact and dimension tables allowing for easy joins and effiecient query performance.

Here are the fact and dimension tables for the schema:
![datamodel.PNG](/datamodel.PNG)


### 3.2 Mapping Out Data Pipelines

1. Load all data files into spark dataframe
2. Drop empty columns,clear null and duplicate rows and remove unneccesary rows
3. Convert date columns from SAS format to datetime
4. Convert state columns in i94 data for invalid values
5. Create fact table fact_immigration from i94 immigration data
6. Create dimension tables from global temperature, US demographics, i94 sas description and airport datasets.
7. Save the fact and dimensions tables in parquet format on S3 for downstream streams


## Step 4: Run Pipelines to Model the Data

### 4.1 Create the data model

We cleaned all the data in spark dataframes in Step 3. Now we save them to fact and dimension tables and write to S3 in parquet format.

**Note:** `etl.py` was created as well to automate the entire data pipeline from start to finish.

### 4.2 Data Quality Checks
Validate all the tables were created on S3 and they are not empty.

*See the Capstone Project.ipynb notebook for more details*


### 4.3 Data dictionary 


**fact_immigration**


| **Column_Name**         | **Column_Description**                                                |
|-------------------------|-----------------------------------------------------------------------|
| cicid                   |  Unique record id                                                     |
| year                    |  Year of arrival                                                      |
| month                   |  Month of arrival                                                     |
| birth_country_code      |  birth country code                                                   |
| residence_country_code  |  residence country code                                               |
| arrival_city_code       |  arrival city code                                                    |
| arrival_date            |  date of arrival                                                      |
| mode_of_transportation  |  mode of transportation. Ex. Air,   Sea, etc.                         |
| arrival_state_code      |  arrival state code                                                   |
| age                     |  Age                                                                  |
| visa_category           |  Visa types categorized in 3   categories:  Business,Student,Pleasure |
| birth_year              |  Year of birth                                                        |
| gender                  |  Immigrants Gender                                                    |
| airline_code            |  Airline code of the airline used   to arrive                         |
| flight_num              |  Airline flight number                                                |
| visa_type               |  Visa type used to enter U.S                                          |
| immigration_id          |  Unique primary key ID                                                |


**dim_country**

| **Column_Name**       | **Column_Description**                            |
|-----------------------|---------------------------------------------------|
| country_code          |  Country Code                                     |
| country_name          |  Country Name                                     |



**dim_state**

| **Column_Name**       | **Column_Description**                            |
|-----------------------|---------------------------------------------------|
| state_code            |  State Code                                       |
| state_name            |  State Name                                       |


**dim_port**

| **Column_Name**       | **Column_Description**                            |
|-----------------------|---------------------------------------------------|
| port_code             |  Port Code                                        |
| port_name             |  Port Name                                        |


**dim_temperature**

| **Column_Name**       | **Column_Description**                            |
|-----------------------|---------------------------------------------------|
| date                  |  Date                                             |
| avg_temp              |  global average land temperature   in celsius    |
| avg_temp_uncertainty  |  the 95% confidence interval around   the average |
| city                  |  Name of City                                     |
| country               |  Name of Country                                  |
| year                  |  Year derived from Date                           |
| month                 |  Month derived from Date                          |


**dim_cities_statistics**

| **Column_Name**       | **Column_Description**                            |
|-----------------------|---------------------------------------------------|
| city                  |  Name of City                                     |
| state                 |  Name of State                                    |
| state_code            |  State Code                                       |
| median_age            |  Median Age of population                         |
| avg_household_size    |  Average city household size                      |


**dim_cities_population**

| **Column_Name**       | **Column_Description**                            |
|-----------------------|---------------------------------------------------|
| city                  |  Name of City                                     |
| state                 |  Name of State                                    |
| state_code            |  State Code                                       |
| male_population       |  Count of Male population                         |
| female_population     |  Count of Female population                       |
| num_of_veterans       |  Count of Total Veterans                          |
| foreign_born          |  Count of residents born in a   different city    |
| race                  |  Respondent race                                  |


**dim_airports**

| **Column_Name**       | **Column_Description**                            |
|-----------------------|---------------------------------------------------|
| airport_code          |  Airport Code                                     |
| airport_type          |  Airport Type                                     |
| airport_name          |  Airport Name                                     |
| continent             |  Continent                                        |
| country               |  Country                                          |
| region                |  Region                                           |
| city                  |  City                                             |


## Step 5: Complete Project Write Up

 
**Tools**:

The immigration data for the fact table consists of ~3M rows each month so Spark was the tool of choice for loading and processing the data. Apache Spark is efficient when it comes to processing large amounts of data. AWS S3 was used for storage since its easily scalable if data volumne increases.


    `AWS S3 for data storage`

    `Python for data processing`
        - Pandas for analysis and processing
        - Spark for processing
    

**Frequency of Data Update**

Immigration data for the fact table is updated monthly so we can update all the data on a monthly cadence. An argument could also be made for a quarterly update since most of the analysis and reports would be looking at trends over a period of time which doesn't require a frequent update of the data.

**Alternate Scenario**

_The data was increased by 100x._
-  We can switch from a standalone Spark cluster to AWS EMR which can handle large datasets on cloud using distributed data clusters.
 
_The data populates a dashboard that must be updated on a daily basis by 7am every day._
-  Apache Airflow can be used to build and run the ETL data pipeline and pass the data to downstream systems for daily dashboard update.
 
_The database needed to be accessed by 100+ people._
-  The final data can be loaded into a Redshift database which can handle multi user access with ease.


## Execution

`dl.cfg`

Add the S3 bucket name and input/output paths in dl.cfg.

Add the S3 credentials in dl.cfg file which will be required to read and write the data in the S3 buckets.

`etl.py`

The script was created the automate all the data pipeline steps show in the Capstone Project.ipynb.

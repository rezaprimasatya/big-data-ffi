# Scope  
This is the project for our **Data Lake** in the(ficticious) company **Sparkify**.  
Our users are avid music lovers and they listen to a lot of music.  
A lot of music means a lot of data. We need to be able to load it all in one place, to analyze it and to improve our offer.  

In this project, we will leverage the use of EMR, Spark and S3 in AWS to build our Data Lake. 
We will also implement a **dimensional model**, to satisfy our analytical needs.  
  
## Technologies  
AWS EMR, Spark, AWS S3, Python, SQL, ETL, Data Lake, Data Warehousing  

## Data Pipeline  
The data pipeline consists of  
1 an initial Extraction phase, from and existing s3 bucket containing logs and song dumps on JSON format  
2 Use Spark to consume and perform aggregation and transformation on those files  
3 Transform data to build dimensions and fact table for the songs listened by our users  
4 Load back the conformed dimensions and fact, storing it on S3 on parquet format

  
#### Dimensions:   
* Users - Sparkify users table  
* Artists - Artists listed on Sparkify  
* Songs - Songs available, with key for corresponding artist  
* Time - date and time dimension  

#### Fact:  
* Songplays - events of songs listened by our users  
  
  
## Files structure  

* dl.cfg  
Setup the necessary AWS keys and configuration, for the process to connect to AWS, as well as input and output path of files.    
    
* manage_emr.ipynb  
Support development and ETL by programmaticaly manage EMR clusters and interact with AWS objects, such as S3.      
   
* check_parquet.ipynb  
Check resulting parquet files, mainly for local testing.      
      
* etl.py  
Run ETL by fetching JSON data, transform and save dimensions and fact table to parquet files.   

* develop.ipynb  
Notebook to develop and help test ETL process step by step.  

#### 1 Running on EMR  

1.1 Configure the necessary credentials (Key and ID) on `dl.cfg` file
1.2 Set up EMR cluster on your AWS account. You can use the UI or the `manage_emr.ipynb` Notebook programmaticaly.  
1.3 Configure the input and output fields on `dl.cfg` file
1.4 Upload `etl.py` and `dl.cfg` to you S3 bucket  
1.5 Execute Spark as step on EMR cluster  
  
_Note:_ For 1.4 and 1.5 you can use AWS UI or check the steps on `manage_emr.ipynb` for doing this programmaticaly  
  
#### 2 Running Locally

2.1 Change credentials and paths for local environment (see sample data under `/data/` folder)  
2.2 Access the terminal and run `python etl.py`  
2.3 Check resulting parquet files using `develop.ipynb` test dataframes  
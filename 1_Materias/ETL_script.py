import logging.config
import time
import psutil
import configparser
import sqlite3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

config = configparser.ConfigParser()
config.read('etlConfig.ini')
JobConfig = config['ETL_Log_Job']

formatter = logging.Formatter('%(levelname)s:  %(asctime)s:  %(process)s:  %(funcName)s:  %(message)s')
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(JobConfig['LogName'])
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def extract():
    logger.info('Start Extract Session')
    #logger.info('Source Filename: {}'.format(JobConfig['SrcObject']))
    try:
        melb_df = pd.read_csv(JobConfig['MelbourneSrcObject'])
        airbnb_df = pd.read_csv(JobConfig['AirbnbSrcObjetc'], low_memory=False)
        logger.info('Records count in Melbourne source file: {}'.format(len(melb_df.index)))
        logger.info('Records count in Airbnb source file: {}'.format(len(airbnb_df.index)))
    except ValueError as e:
        logger.error(e)
        return
    logger.info("Read completed")
    return melb_df, airbnb_df

def transform(melb_df, airbnb_df):
    try:
        airbnb_df['zipcode'] = pd.to_numeric(airbnb_df.zipcode, errors='coerce')
        airbnb_df['zipcode'] = airbnb_df.zipcode.fillna(0).astype('int')

        airbnb_grouped = airbnb_df.groupby('zipcode').agg(
            airbnb_price_mean=('price', 'mean'),
            airbnb_record_count=('zipcode', 'count'),
            airbnb_weekly_price_mean=('weekly_price', 'mean'),
            airbnb_monthly_price_mean=('monthly_price', 'mean')
            ).reset_index()

        melb_df['Postcode'] = melb_df['Postcode'].astype(int)
        merged_df = pd.merge(melb_df, airbnb_grouped, left_on='Postcode', right_on='zipcode', how='left')

        logger.info('Transformation completed, data ready to load')

    except Exception as e:
        logger.error(e)
        return None

    return merged_df

def load(ldf):
    logger.info('Start Load Session')
    try:
        conn = sqlite3.connect(JobConfig['TgtConnection'])
        cursor = conn.cursor()
        logger.info('Connection to {} database established'.format(JobConfig['TgtConnection']))
    except Exception as e:
        logger.error(e)
        return

    #Load dataframe to table
    try:
        ldf.to_sql(JobConfig['TgtObject'], conn, if_exists='replace', index=False)
        logger.info("Data Loaded into target table: {}".format(JobConfig['TgtObject']))
    except Exception as e:
        logger.error(e)
        return
    conn.commit()
    logger.info("Data Loaded into target table: {}".format(JobConfig['TgtObject']))
    return

def main():
    start = time.time()

    ##Extract
    start1 = time.time()
    melb_df, airbnb_df = extract()
    end1 = time.time() - start1
    logger.info('Extract CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info("Extract function took : {} seconds".format(end1))

    ##Transformation
    start2 = time.time()
    ldf = transform(melb_df, airbnb_df)
    end2 = time.time() - start2
    logger.info('Transform CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info("Transformation took : {} seconds".format(end2))

    ##Load
    start3 = time.time()
    load(ldf)
    end3 = time.time() - start3
    logger.info('Load CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info("Load took : {} seconds".format(end3))
    end = time.time() - start
    logger.info("ETL Job took : {} seconds".format(end))
    logger.info('Session Summary')
    logger.info('RAM memory {}% used:'.format(psutil.virtual_memory().percent))
    logger.info('CPU usage {}%'.format(psutil.cpu_percent()))
    print("multiple threads took : {} seconds".format(end))

if __name__=="__main__":
    logger.info('ETL Process Initialized')
    main()

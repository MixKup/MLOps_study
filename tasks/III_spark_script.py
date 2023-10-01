'''USAGE: чтобы заработали креды, нужно создать в коре .aws папку. в ней два факйла:
credentials:
[default]
  aws_access_key_id = YCAJEI1C63_Fj5X5Lkw80GOLr
  aws_secret_access_key = YCMexGNeTMBjKZWdsYw0SPHWOyShoHuI8CtA0-uV

config:
[default]
  region=ru-central1
'''
import findspark
findspark.init()

import boto3
import pyspark
from pyspark.sql.types import StructField, IntegerType, StructType, StringType, FloatType
from pyspark.ml import Pipeline, Transformer
from pyspark.sql.functions import col,lit
from pyspark.ml.feature import Imputer
from pyspark.ml.feature import StringIndexer, OneHotEncoder


class ValidationIds(Transformer):
    def __init__(self):
        super(ValidationIds, self).__init__()
        self.counter = {'tranaction_id': 1, 'customer_id': 1, 'terminal_id': 1}

    def _transform(self, df):
        df = df.dropna(subset=list(self.counter.keys())) # drop nans

        for colid in self.counter.keys(): # edit non-digits
            dq = df.filter(df[colid].rlike('\D+'))
            df = df[~df[colid].rlike('\D+')]
            dq = dq.withColumn(colid, lit('undefinded_' + colid.split('id')[0] + str(self.counter[colid])))
            self.counter[colid] += 1
            df = df.union(dq)

        return df


class ValidationNumerics(Transformer):
    def __init__(self, num_list):
        super(ValidationNumerics, self).__init__()
        self.num_list = num_list

    def _transform(self, df):
        for cols in self.num_list:
            df = df[df[cols] > 0] # time has to be > 0
            lower_quan, upper_quan = df.approxQuantile(cols, [0.25, 0.75], 0.05)
            iqr = upper_quan - lower_quan
            lower_extreme = lower_quan - 1.5*iqr
            upper_extreme = upper_quan + 1.5*iqr
            dfu = df.where(df[cols] < upper_extreme)
            df = dfu.where(dfu[cols] > lower_extreme) # delete outliers

        return df


class ValidationBinary(Transformer):
    def __init__(self, bin_list):
        super(ValidationBinary, self).__init__()
        self.bin_list = bin_list

    def _transform(self, df):
        for colb in self.bin_list:
            df_not_0 = df[df[colb] != 0]
            df_only_0 = df[df[colb] == 0]
            df_only_1 = df[df[colb] == 1]
            df_not_10 = df_not_0[df_not_0[colb] != 1] # df10 contains not 0 and not 1
            if df_only_0.count() > df_only_1.count():
                df_not_10 = df_not_10.withColumn(colb, lit(0))
            else:
                df_not_10 = df_not_10.withColumn(colb, lit(1))

            df = df_only_0.union(df_only_1)
            df = df.union(df_not_10)
        return df


session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net'
)

#s3 = session.resource('s3')
#bucket = s3.Bucket('mlops-course-bucket')
#print(bucket.objects.all())

bucket_name = 'mlops-course-bucket'
folder = 'second_hw'

filenames = [key['Key'] for key in s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{folder}/')['Contents']]

#get_object_response = s3.get_object(Bucket='bucket-name',Key='second_hw/2020-05-18.txt')
#print(get_object_response['Body'].read())
list_of_processed_files = set(list())

list_of_new_files = [x for x in filenames if x not in list_of_processed_files]
print(f'list of new files is {list_of_new_files}')

spark = (
    pyspark.sql.SparkSession.builder
        .appName('MK')
        .master("local[2]") # limit executor to 2 cores
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "8g")
        .config("spark.ui.port", '4040')
        .getOrCreate()
)

newDF=[StructField('tranaction_id',StringType(),True),
       StructField('tx_datetime',StringType(),True),
       StructField('customer_id',StringType(),True),
       StructField('terminal_id',StringType(),True),
       StructField('tx_amount',FloatType(),True),
       StructField('tx_time_seconds',IntegerType(),True),
       StructField('tx_time_days',IntegerType(),True),
       StructField('tx_fraud',IntegerType(),True),
       StructField('tx_fraud_scenario',IntegerType(),True)
       ]
finalStruct=StructType(fields=newDF)

amount_imputer = Imputer(inputCol="tx_amount", outputCol="tx_amount", strategy="mean")
time_seconds_imputer = Imputer(inputCol="tx_time_seconds", outputCol="tx_time_seconds", strategy="mean")
time_days_imputer = Imputer(inputCol="tx_time_days", outputCol="tx_time_days", strategy="mean")

fraud_scenario_indexer = StringIndexer(inputCol="tx_fraud_scenario", outputCol="tx_fraud_scenario_index")
fraud_scenario_encoder = OneHotEncoder(inputCol="tx_fraud_scenario_index", outputCol="tx_fraud_scenario_encoded")


for name in list_of_new_files:
    df = spark.read.csv(f's3a://{bucket_name}/{name}',schema=finalStruct, header=True)

    df.show(5)

    vids = ValidationIds()
    vnum = ValidationNumerics(['tx_amount', 'tx_time_seconds', 'tx_time_days'])
    amount_imputer = Imputer(inputCol="tx_amount", outputCol="tx_amount", strategy="mean")
    time_seconds_imputer = Imputer(inputCol="tx_time_seconds", outputCol="tx_time_seconds", strategy="mean")
    time_days_imputer = Imputer(inputCol="tx_time_days", outputCol="tx_time_days", strategy="mean")
    vbin = ValidationBinary(['tx_fraud'])
    fraud_scenario_indexer = StringIndexer(inputCol="tx_fraud_scenario", outputCol="tx_fraud_scenario_index")
    fraud_scenario_encoder = OneHotEncoder(inputCol="tx_fraud_scenario_index", outputCol="tx_fraud_scenario_encoded")

    feat_ext_pipe = Pipeline(stages=[
        vids,
        vnum,
        amount_imputer,
        time_seconds_imputer,
        time_days_imputer,
        vbin,
        fraud_scenario_indexer,
        fraud_scenario_encoder
        ]).fit(df)



    feat_df = feat_ext_pipe.transform(df)

    feat_df.show(5)

    feat_df.write.mode("overwrite").parquet(f's3a://{bucket_name}/third_hw/{name.split(".txt")[0]}.parquet')

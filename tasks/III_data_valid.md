скрипт для подтягивания и преобразования данных в спарке

```python
import findspark
findspark.init()
import pyspark

spark = (
    pyspark.sql.SparkSession.builder
        .appName('MK')
        .master("local[2]") # limit executor to 2 cores
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "8g")
        .config("spark.ui.port", '4040')
        .getOrCreate()
)
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)


from pyspark.sql.types import StructField, IntegerType, StructType, StringType, FloatType
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
df = spark.read.csv('s3a://mlops-course-bucket/second_hw/2019-09-21.txt',schema=finalStruct, header=True)

df.show(5)

print(df.printSchema())
print(df.dtypes)
```
![image](https://github.com/MixKup/MLOps_study/assets/19960794/ab28b697-a4a2-4886-b306-c584cf3ad351)

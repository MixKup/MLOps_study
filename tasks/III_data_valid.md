скрипт для подтягивания и преобразования данных в спарке

```python
from pyspark import SparkContext, SparkConf
from pyspark. sql import SQLContext
from pyspark. sql import SparkSession


spark = SparkSession.builder.appName('spark-sql').getOrCreate()
sqlContext = SQLContext(spark)

sc = spark.sparkContext
lines = sc.textFile('s3a://mlops-course-bucket/second_hw/2019-08-22.txt')
header = lines.first()

temp = lines.map(lambda k: k.split(",") if k!=header else ['','','','','','','','','']) # FIXME временное решение
df = temp.toDF(header.split("|"))

print(df.show())
```

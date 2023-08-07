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
![image](https://github.com/MixKup/MLOps_study/assets/19960794/ab28b697-a4a2-4886-b306-c584cf3ad351)

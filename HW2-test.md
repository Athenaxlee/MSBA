

```python
import time
import pyspark
import pandas as pd
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import Row
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
```


```python
sqlContext = SQLContext(sc)
```


```python
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('hdfs:///user/training/mldata/titanic_train.csv')
#rawdata = sc.textFile("hdfs:///user/training/mldata/titanic_train.csv")
```


```python
df.printSchema()
```

    root
     |-- PassengerId: string (nullable = true)
     |-- Survived: string (nullable = true)
     |-- Pclass: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Sex: string (nullable = true)
     |-- Age: string (nullable = true)
     |-- SibSp: string (nullable = true)
     |-- Parch: string (nullable = true)
     |-- Ticket: string (nullable = true)
     |-- Fare: string (nullable = true)
     |-- Cabin: string (nullable = true)
     |-- Embarked: string (nullable = true)
    



```python
df.head(10)
```




    [Row(PassengerId=u'1', Survived=u'0', Pclass=u'3', Name=u'Braund, Mr. Owen Harris', Sex=u'male', Age=u'22', SibSp=u'1', Parch=u'0', Ticket=u'A/5 21171', Fare=u'7.25', Cabin=None, Embarked=u'S'),
     Row(PassengerId=u'2', Survived=u'1', Pclass=u'1', Name=u'Cumings, Mrs. John Bradley (Florence Briggs Thayer)', Sex=u'female', Age=u'38', SibSp=u'1', Parch=u'0', Ticket=u'PC 17599', Fare=u'71.2833', Cabin=u'C85', Embarked=u'C'),
     Row(PassengerId=u'3', Survived=u'1', Pclass=u'3', Name=u'Heikkinen, Miss. Laina', Sex=u'female', Age=u'26', SibSp=u'0', Parch=u'0', Ticket=u'STON/O2. 3101282', Fare=u'7.925', Cabin=None, Embarked=u'S'),
     Row(PassengerId=u'4', Survived=u'1', Pclass=u'1', Name=u'Futrelle, Mrs. Jacques Heath (Lily May Peel)', Sex=u'female', Age=u'35', SibSp=u'1', Parch=u'0', Ticket=u'113803', Fare=u'53.1', Cabin=u'C123', Embarked=u'S'),
     Row(PassengerId=u'5', Survived=u'0', Pclass=u'3', Name=u'Allen, Mr. William Henry', Sex=u'male', Age=u'35', SibSp=u'0', Parch=u'0', Ticket=u'373450', Fare=u'8.05', Cabin=None, Embarked=u'S'),
     Row(PassengerId=u'6', Survived=u'0', Pclass=u'3', Name=u'Moran, Mr. James', Sex=u'male', Age=None, SibSp=u'0', Parch=u'0', Ticket=u'330877', Fare=u'8.4583', Cabin=None, Embarked=u'Q'),
     Row(PassengerId=u'7', Survived=u'0', Pclass=u'1', Name=u'McCarthy, Mr. Timothy J', Sex=u'male', Age=u'54', SibSp=u'0', Parch=u'0', Ticket=u'17463', Fare=u'51.8625', Cabin=u'E46', Embarked=u'S'),
     Row(PassengerId=u'8', Survived=u'0', Pclass=u'3', Name=u'Palsson, Master. Gosta Leonard', Sex=u'male', Age=u'2', SibSp=u'3', Parch=u'1', Ticket=u'349909', Fare=u'21.075', Cabin=None, Embarked=u'S'),
     Row(PassengerId=u'9', Survived=u'1', Pclass=u'3', Name=u'Johnson, Mrs. Oscar W (Elisabeth Vilhelmina Berg)', Sex=u'female', Age=u'27', SibSp=u'0', Parch=u'2', Ticket=u'347742', Fare=u'11.1333', Cabin=None, Embarked=u'S'),
     Row(PassengerId=u'10', Survived=u'1', Pclass=u'2', Name=u'Nasser, Mrs. Nicholas (Adele Achem)', Sex=u'female', Age=u'14', SibSp=u'1', Parch=u'0', Ticket=u'237736', Fare=u'30.0708', Cabin=None, Embarked=u'C')]




```python
df.describe().show()
```

    +-------+-----------------+-------------------+------------------+--------------------+------+------------------+------------------+-------------------+------------------+-----------------+-----+--------+
    |summary|      PassengerId|           Survived|            Pclass|                Name|   Sex|               Age|             SibSp|              Parch|            Ticket|             Fare|Cabin|Embarked|
    +-------+-----------------+-------------------+------------------+--------------------+------+------------------+------------------+-------------------+------------------+-----------------+-----+--------+
    |  count|              891|                891|               891|                 891|   891|               714|               891|                891|               891|              891|  204|     889|
    |   mean|            446.0| 0.3838383838383838| 2.308641975308642|                null|  null| 29.69911764705882|0.5230078563411896|0.38159371492704824|260318.54916792738| 32.2042079685746| null|    null|
    | stddev|257.3538420152301|0.48659245426485753|0.8360712409770491|                null|  null|14.526497332334035|1.1027434322934315| 0.8060572211299488|471609.26868834975|49.69342859718089| null|    null|
    |    min|                1|                  0|                 1|"Andersson, Mr. A...|female|              0.42|                 0|                  0|            110152|                0|  A10|       C|
    |    max|               99|                  1|                 3|van Melkebeke, Mr...|  male|                 9|                 8|                  6|         WE/P 5735|             93.5|    T|       S|
    +-------+-----------------+-------------------+------------------+--------------------+------+------------------+------------------+-------------------+------------------+-----------------+-----+--------+
    



```python
import pandas as pd
dfp = df.toPandas()
```


```python
dfp.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>PassengerId</th>
      <th>Survived</th>
      <th>Pclass</th>
      <th>Name</th>
      <th>Sex</th>
      <th>Age</th>
      <th>SibSp</th>
      <th>Parch</th>
      <th>Ticket</th>
      <th>Fare</th>
      <th>Cabin</th>
      <th>Embarked</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>0</td>
      <td>3</td>
      <td>Braund, Mr. Owen Harris</td>
      <td>male</td>
      <td>22</td>
      <td>1</td>
      <td>0</td>
      <td>A/5 21171</td>
      <td>7.25</td>
      <td>None</td>
      <td>S</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>1</td>
      <td>Cumings, Mrs. John Bradley (Florence Briggs Th...</td>
      <td>female</td>
      <td>38</td>
      <td>1</td>
      <td>0</td>
      <td>PC 17599</td>
      <td>71.2833</td>
      <td>C85</td>
      <td>C</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>3</td>
      <td>Heikkinen, Miss. Laina</td>
      <td>female</td>
      <td>26</td>
      <td>0</td>
      <td>0</td>
      <td>STON/O2. 3101282</td>
      <td>7.925</td>
      <td>None</td>
      <td>S</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>Futrelle, Mrs. Jacques Heath (Lily May Peel)</td>
      <td>female</td>
      <td>35</td>
      <td>1</td>
      <td>0</td>
      <td>113803</td>
      <td>53.1</td>
      <td>C123</td>
      <td>S</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>0</td>
      <td>3</td>
      <td>Allen, Mr. William Henry</td>
      <td>male</td>
      <td>35</td>
      <td>0</td>
      <td>0</td>
      <td>373450</td>
      <td>8.05</td>
      <td>None</td>
      <td>S</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>0</td>
      <td>3</td>
      <td>Moran, Mr. James</td>
      <td>male</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>330877</td>
      <td>8.4583</td>
      <td>None</td>
      <td>Q</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>0</td>
      <td>1</td>
      <td>McCarthy, Mr. Timothy J</td>
      <td>male</td>
      <td>54</td>
      <td>0</td>
      <td>0</td>
      <td>17463</td>
      <td>51.8625</td>
      <td>E46</td>
      <td>S</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>0</td>
      <td>3</td>
      <td>Palsson, Master. Gosta Leonard</td>
      <td>male</td>
      <td>2</td>
      <td>3</td>
      <td>1</td>
      <td>349909</td>
      <td>21.075</td>
      <td>None</td>
      <td>S</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>1</td>
      <td>3</td>
      <td>Johnson, Mrs. Oscar W (Elisabeth Vilhelmina Berg)</td>
      <td>female</td>
      <td>27</td>
      <td>0</td>
      <td>2</td>
      <td>347742</td>
      <td>11.1333</td>
      <td>None</td>
      <td>S</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>1</td>
      <td>2</td>
      <td>Nasser, Mrs. Nicholas (Adele Achem)</td>
      <td>female</td>
      <td>14</td>
      <td>1</td>
      <td>0</td>
      <td>237736</td>
      <td>30.0708</td>
      <td>None</td>
      <td>C</td>
    </tr>
  </tbody>
</table>
</div>




```python
#summary of the dataframe
summary = dfp.describe()
summary.transpose()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count</th>
      <th>unique</th>
      <th>top</th>
      <th>freq</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>PassengerId</th>
      <td>891</td>
      <td>891</td>
      <td>823</td>
      <td>1</td>
    </tr>
    <tr>
      <th>Survived</th>
      <td>891</td>
      <td>2</td>
      <td>0</td>
      <td>549</td>
    </tr>
    <tr>
      <th>Pclass</th>
      <td>891</td>
      <td>3</td>
      <td>3</td>
      <td>491</td>
    </tr>
    <tr>
      <th>Name</th>
      <td>891</td>
      <td>891</td>
      <td>Graham, Mr. George Edward</td>
      <td>1</td>
    </tr>
    <tr>
      <th>Sex</th>
      <td>891</td>
      <td>2</td>
      <td>male</td>
      <td>577</td>
    </tr>
    <tr>
      <th>Age</th>
      <td>714</td>
      <td>88</td>
      <td>24</td>
      <td>30</td>
    </tr>
    <tr>
      <th>SibSp</th>
      <td>891</td>
      <td>7</td>
      <td>0</td>
      <td>608</td>
    </tr>
    <tr>
      <th>Parch</th>
      <td>891</td>
      <td>7</td>
      <td>0</td>
      <td>678</td>
    </tr>
    <tr>
      <th>Ticket</th>
      <td>891</td>
      <td>681</td>
      <td>CA. 2343</td>
      <td>7</td>
    </tr>
    <tr>
      <th>Fare</th>
      <td>891</td>
      <td>248</td>
      <td>8.05</td>
      <td>43</td>
    </tr>
    <tr>
      <th>Cabin</th>
      <td>204</td>
      <td>147</td>
      <td>C23 C25 C27</td>
      <td>4</td>
    </tr>
    <tr>
      <th>Embarked</th>
      <td>889</td>
      <td>3</td>
      <td>S</td>
      <td>644</td>
    </tr>
  </tbody>
</table>
</div>




```python
#only age, cabin, and embarked has null values
dfp.isnull().sum()
```




    PassengerId      0
    Survived         0
    Pclass           0
    Name             0
    Sex              0
    Age            177
    SibSp            0
    Parch            0
    Ticket           0
    Fare             0
    Cabin          687
    Embarked         2
    dtype: int64




```python
ctsex = dfp['Sex'].value_counts()
ctcabin = dfp['Cabin'].value_counts()
ctembark = dfp['Embarked'].value_counts()
ctsurvived = dfp['Survived'].value_counts()
ctPclass = dfp['Pclass'].value_counts()
ctage = dfp['Age'].value_counts()
ctsibsp = dfp['SibSp'].value_counts()
ctparch = dfp['Parch'].value_counts()
ctsex.head(10)
```




    male      577
    female    314
    Name: Sex, dtype: int64




```python
ctcabin.head(10)
#I will drop Cabin column, because it has the most null values and its values are too spa
```




    C23 C25 C27        4
    G6                 4
    B96 B98            4
    D                  3
    C22 C26            3
    E101               3
    F2                 3
    F33                3
    B57 B59 B63 B66    2
    C68                2
    Name: Cabin, dtype: int64




```python
ctembark.head(10)
```




    S    644
    C    168
    Q     77
    Name: Embarked, dtype: int64




```python
ctsurvived.head(10)
```




    0    549
    1    342
    Name: Survived, dtype: int64




```python
ctPclass.head(10)
```




    3    491
    1    216
    2    184
    Name: Pclass, dtype: int64




```python
ctage.head(10)
```




    24    30
    22    27
    18    26
    30    25
    28    25
    19    25
    21    24
    25    23
    36    22
    29    20
    Name: Age, dtype: int64




```python
ctsibsp.head(10)
```




    0    608
    1    209
    2     28
    4     18
    3     16
    8      7
    5      5
    Name: SibSp, dtype: int64




```python
ctparch.head(10)
```




    0    678
    1    118
    2     80
    5      5
    3      5
    4      4
    6      1
    Name: Parch, dtype: int64




```python
dfsl = sqlContext.createDataFrame(dfp.iloc[:,[1,2,4,5,6,7,9,11]]).show()
```

    +--------+------+------+----+-----+-----+-------+--------+
    |Survived|Pclass|   Sex| Age|SibSp|Parch|   Fare|Embarked|
    +--------+------+------+----+-----+-----+-------+--------+
    |       0|     3|  male|  22|    1|    0|   7.25|       S|
    |       1|     1|female|  38|    1|    0|71.2833|       C|
    |       1|     3|female|  26|    0|    0|  7.925|       S|
    |       1|     1|female|  35|    1|    0|   53.1|       S|
    |       0|     3|  male|  35|    0|    0|   8.05|       S|
    |       0|     3|  male|null|    0|    0| 8.4583|       Q|
    |       0|     1|  male|  54|    0|    0|51.8625|       S|
    |       0|     3|  male|   2|    3|    1| 21.075|       S|
    |       1|     3|female|  27|    0|    2|11.1333|       S|
    |       1|     2|female|  14|    1|    0|30.0708|       C|
    |       1|     3|female|   4|    1|    1|   16.7|       S|
    |       1|     1|female|  58|    0|    0|  26.55|       S|
    |       0|     3|  male|  20|    0|    0|   8.05|       S|
    |       0|     3|  male|  39|    1|    5| 31.275|       S|
    |       0|     3|female|  14|    0|    0| 7.8542|       S|
    |       1|     2|female|  55|    0|    0|     16|       S|
    |       0|     3|  male|   2|    4|    1| 29.125|       Q|
    |       1|     2|  male|null|    0|    0|     13|       S|
    |       0|     3|female|  31|    1|    0|     18|       S|
    |       1|     3|female|null|    0|    0|  7.225|       C|
    +--------+------+------+----+-----+-----+-------+--------+
    only showing top 20 rows
    


## Step 3:engineer the necessary features for the machine learning model


```python
from pyspark.sql.types import *
from pyspark.sql.functions import *
```


```python
# String to double on some columns of the dataset : creates a new dataset
dfls = df.select(col("Survived").cast("double"),col("Sex"),col("Embarked"),col("Parch").cast("double"),col("Pclass").cast("double"),col("Age").cast("double"),col("SibSp").cast("double"),col("Fare").cast("double"),col("Age").cast("double").alias("AgeNA"))
```


```python
#now we take a look at the new dataframe with selected columns converted to double type
dfls.printSchema()
```

    root
     |-- Survived: double (nullable = true)
     |-- Sex: string (nullable = true)
     |-- Embarked: string (nullable = true)
     |-- Parch: double (nullable = true)
     |-- Pclass: double (nullable = true)
     |-- Age: double (nullable = true)
     |-- SibSp: double (nullable = true)
     |-- Fare: double (nullable = true)
     |-- AgeNA: double (nullable = true)
    



```python
avgage = dfls.select([mean('Age')]).show()
```

    +-----------------+
    |         avg(Age)|
    +-----------------+
    |29.69911764705882|
    +-----------------+
    



```python
#Replace the missing values in the Age column with the mean value
dfillna = dfls.na.fill({'Age': 29.7,'AgeNA': 1})
dfillna.show()
```

    +--------+------+--------+-----+------+----+-----+-------+-----+
    |Survived|   Sex|Embarked|Parch|Pclass| Age|SibSp|   Fare|AgeNA|
    +--------+------+--------+-----+------+----+-----+-------+-----+
    |     0.0|  male|       S|  0.0|   3.0|22.0|  1.0|   7.25| 22.0|
    |     1.0|female|       C|  0.0|   1.0|38.0|  1.0|71.2833| 38.0|
    |     1.0|female|       S|  0.0|   3.0|26.0|  0.0|  7.925| 26.0|
    |     1.0|female|       S|  0.0|   1.0|35.0|  1.0|   53.1| 35.0|
    |     0.0|  male|       S|  0.0|   3.0|35.0|  0.0|   8.05| 35.0|
    |     0.0|  male|       Q|  0.0|   3.0|29.7|  0.0| 8.4583|  1.0|
    |     0.0|  male|       S|  0.0|   1.0|54.0|  0.0|51.8625| 54.0|
    |     0.0|  male|       S|  1.0|   3.0| 2.0|  3.0| 21.075|  2.0|
    |     1.0|female|       S|  2.0|   3.0|27.0|  0.0|11.1333| 27.0|
    |     1.0|female|       C|  0.0|   2.0|14.0|  1.0|30.0708| 14.0|
    |     1.0|female|       S|  1.0|   3.0| 4.0|  1.0|   16.7|  4.0|
    |     1.0|female|       S|  0.0|   1.0|58.0|  0.0|  26.55| 58.0|
    |     0.0|  male|       S|  0.0|   3.0|20.0|  0.0|   8.05| 20.0|
    |     0.0|  male|       S|  5.0|   3.0|39.0|  1.0| 31.275| 39.0|
    |     0.0|female|       S|  0.0|   3.0|14.0|  0.0| 7.8542| 14.0|
    |     1.0|female|       S|  0.0|   2.0|55.0|  0.0|   16.0| 55.0|
    |     0.0|  male|       Q|  1.0|   3.0| 2.0|  4.0| 29.125|  2.0|
    |     1.0|  male|       S|  0.0|   2.0|29.7|  0.0|   13.0|  1.0|
    |     0.0|female|       S|  0.0|   3.0|31.0|  1.0|   18.0| 31.0|
    |     1.0|female|       C|  0.0|   3.0|29.7|  0.0|  7.225|  1.0|
    +--------+------+--------+-----+------+----+-----+-------+-----+
    only showing top 20 rows
    



```python
newdf = dfillna.select(col("Survived"),col("Sex"),col("Embarked"),col("Parch"),col("Pclass"),col("Age"),col("SibSp"),col("Fare"),\
when(dfillna["Age"] != 29.7, 0).otherwise(1).alias("AgeNA"))
newdf.show()
```

    +--------+------+--------+-----+------+----+-----+-------+-----+
    |Survived|   Sex|Embarked|Parch|Pclass| Age|SibSp|   Fare|AgeNA|
    +--------+------+--------+-----+------+----+-----+-------+-----+
    |     0.0|  male|       S|  0.0|   3.0|22.0|  1.0|   7.25|    0|
    |     1.0|female|       C|  0.0|   1.0|38.0|  1.0|71.2833|    0|
    |     1.0|female|       S|  0.0|   3.0|26.0|  0.0|  7.925|    0|
    |     1.0|female|       S|  0.0|   1.0|35.0|  1.0|   53.1|    0|
    |     0.0|  male|       S|  0.0|   3.0|35.0|  0.0|   8.05|    0|
    |     0.0|  male|       Q|  0.0|   3.0|29.7|  0.0| 8.4583|    1|
    |     0.0|  male|       S|  0.0|   1.0|54.0|  0.0|51.8625|    0|
    |     0.0|  male|       S|  1.0|   3.0| 2.0|  3.0| 21.075|    0|
    |     1.0|female|       S|  2.0|   3.0|27.0|  0.0|11.1333|    0|
    |     1.0|female|       C|  0.0|   2.0|14.0|  1.0|30.0708|    0|
    |     1.0|female|       S|  1.0|   3.0| 4.0|  1.0|   16.7|    0|
    |     1.0|female|       S|  0.0|   1.0|58.0|  0.0|  26.55|    0|
    |     0.0|  male|       S|  0.0|   3.0|20.0|  0.0|   8.05|    0|
    |     0.0|  male|       S|  5.0|   3.0|39.0|  1.0| 31.275|    0|
    |     0.0|female|       S|  0.0|   3.0|14.0|  0.0| 7.8542|    0|
    |     1.0|female|       S|  0.0|   2.0|55.0|  0.0|   16.0|    0|
    |     0.0|  male|       Q|  1.0|   3.0| 2.0|  4.0| 29.125|    0|
    |     1.0|  male|       S|  0.0|   2.0|29.7|  0.0|   13.0|    1|
    |     0.0|female|       S|  0.0|   3.0|31.0|  1.0|   18.0|    0|
    |     1.0|female|       C|  0.0|   3.0|29.7|  0.0|  7.225|    1|
    +--------+------+--------+-----+------+----+-----+-------+-----+
    only showing top 20 rows
    



```python
#recalculate the summary statistics.
summarynew = newdf.toPandas().describe()
summarynew.transpose()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count</th>
      <th>mean</th>
      <th>std</th>
      <th>min</th>
      <th>25%</th>
      <th>50%</th>
      <th>75%</th>
      <th>max</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Survived</th>
      <td>891.00</td>
      <td>0.38</td>
      <td>0.49</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>1.00</td>
      <td>1.00</td>
    </tr>
    <tr>
      <th>Parch</th>
      <td>891.00</td>
      <td>0.38</td>
      <td>0.81</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>6.00</td>
    </tr>
    <tr>
      <th>Pclass</th>
      <td>891.00</td>
      <td>2.31</td>
      <td>0.84</td>
      <td>1.00</td>
      <td>2.00</td>
      <td>3.00</td>
      <td>3.00</td>
      <td>3.00</td>
    </tr>
    <tr>
      <th>Age</th>
      <td>891.00</td>
      <td>29.70</td>
      <td>13.00</td>
      <td>0.42</td>
      <td>22.00</td>
      <td>29.70</td>
      <td>35.00</td>
      <td>80.00</td>
    </tr>
    <tr>
      <th>SibSp</th>
      <td>891.00</td>
      <td>0.52</td>
      <td>1.10</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>1.00</td>
      <td>8.00</td>
    </tr>
    <tr>
      <th>Fare</th>
      <td>891.00</td>
      <td>32.20</td>
      <td>49.69</td>
      <td>0.00</td>
      <td>7.91</td>
      <td>14.45</td>
      <td>31.00</td>
      <td>512.33</td>
    </tr>
    <tr>
      <th>AgeNA</th>
      <td>891.00</td>
      <td>0.20</td>
      <td>0.40</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>1.00</td>
    </tr>
  </tbody>
</table>
</div>



## Step 4: Encode all string and categorical variables in order to use them in the pipeline afterwards


```python
#Create StringIndexer transformers; StringIndexer encodes a string column of labels to a column of label indices
indexedcols = ["sexVec","embarkedVec","Parch", "Pclass", "Age","SibSp","Fare","AgeNA"]
sex_indexer = StringIndexer(inputCol="Sex",outputCol="indexedSex").fit(newdf)
embarked_indexer = StringIndexer(inputCol="Embarked",outputCol="indexedEmbarked").fit(newdf)
surviveIndexer = StringIndexer(inputCol="Survived", outputCol="indexedSurvived")
```


```python
# One Hot Encoder on indexed features
sex_encoder = OneHotEncoder(inputCol="indexedSex", outputCol="sexVec")
embarked_encoder = OneHotEncoder(inputCol="indexedEmbarked", outputCol="embarkedVec")
```

## Step 5: Assemble all feature columns into a feature vector in order to be used in the pipeline


```python
va = VectorAssembler(inputCols = indexedcols, outputCol = 'features')
```

## Step 6: Create the logistic regression model to be used in the pipeline


```python
# Train a Logistic regression model.
lgm = LogisticRegression(labelCol="indexedSurvived", featuresCol="features", maxIter=10)
```

## Step 7: assemble the pipeline


```python
# Chain indexers and logistic regression actions in a Pipeline
pipeline = Pipeline(stages=[surviveIndexer, sex_indexer, embarked_indexer, sex_encoder,embarked_encoder,va,lgm])
```

## Step 8: prepare the training and test datasets


```python
# splitting dataset into train and test set
(trainData, testData) = newdf.randomSplit([0.7, 0.3])
```


```python
#0.7 of the original data set
trainData.count()
```




    617




```python
#0.3 of the data set
testData.count() 
```




    274




```python
trainData.show()
```

    +--------+------+--------+-----+------+----+-----+-------+-----+
    |Survived|   Sex|Embarked|Parch|Pclass| Age|SibSp|   Fare|AgeNA|
    +--------+------+--------+-----+------+----+-----+-------+-----+
    |     0.0|female|       C|  0.0|   1.0|50.0|  0.0|28.7125|    0|
    |     0.0|female|       C|  0.0|   3.0|14.5|  1.0|14.4542|    0|
    |     0.0|female|       C|  0.0|   3.0|29.7|  1.0|14.4542|    1|
    |     0.0|female|       C|  1.0|   3.0|18.0|  0.0|14.4542|    0|
    |     0.0|female|       C|  2.0|   3.0|29.7|  0.0|15.2458|    1|
    |     0.0|female|       Q|  0.0|   3.0|18.0|  0.0|   6.75|    0|
    |     0.0|female|       Q|  0.0|   3.0|21.0|  0.0|   7.75|    0|
    |     0.0|female|       Q|  0.0|   3.0|29.7|  0.0| 7.6292|    1|
    |     0.0|female|       Q|  0.0|   3.0|29.7|  0.0|   7.75|    1|
    |     0.0|female|       Q|  0.0|   3.0|30.5|  0.0|   7.75|    0|
    |     0.0|female|       Q|  1.0|   3.0|32.0|  1.0|   15.5|    0|
    |     0.0|female|       Q|  2.0|   3.0|29.7|  0.0|   7.75|    1|
    |     0.0|female|       Q|  5.0|   3.0|39.0|  0.0| 29.125|    0|
    |     0.0|female|       S|  0.0|   2.0|24.0|  0.0|   13.0|    0|
    |     0.0|female|       S|  0.0|   2.0|38.0|  0.0|   13.0|    0|
    |     0.0|female|       S|  0.0|   2.0|57.0|  0.0|   10.5|    0|
    |     0.0|female|       S|  0.0|   3.0|18.0|  0.0|  7.775|    0|
    |     0.0|female|       S|  0.0|   3.0|18.0|  1.0|   17.8|    0|
    |     0.0|female|       S|  0.0|   3.0|18.0|  2.0|   18.0|    0|
    |     0.0|female|       S|  0.0|   3.0|20.0|  1.0|  9.825|    0|
    +--------+------+--------+-----+------+----+-----+-------+-----+
    only showing top 20 rows
    


## Step 9: Fit the model and then use it on the test set to generate predictions


```python
model = pipeline.fit(trainData)
```


```python
#logistic regression coefficients
#the odds of your outcome for survived are exp(-2.7938) = 1.01 times that of the odds of not survived
[stage.coefficients for stage in model.stages if hasattr(stage, "coefficients")]
```




    [DenseVector([-2.7938, 1.1055, 1.2704, -0.0512, -0.4872, -0.0395, -0.4662, 0.0144, 0.1406])]




```python
predictions = model.transform(testData)
```


```python
predictions.printSchema()
```

    root
     |-- Survived: double (nullable = true)
     |-- Sex: string (nullable = true)
     |-- Embarked: string (nullable = true)
     |-- Parch: double (nullable = true)
     |-- Pclass: double (nullable = true)
     |-- Age: double (nullable = false)
     |-- SibSp: double (nullable = true)
     |-- Fare: double (nullable = true)
     |-- AgeNA: integer (nullable = false)
     |-- indexedSurvived: double (nullable = true)
     |-- indexedSex: double (nullable = true)
     |-- indexedEmbarked: double (nullable = true)
     |-- sexVec: vector (nullable = true)
     |-- embarkedVec: vector (nullable = true)
     |-- features: vector (nullable = true)
     |-- rawPrediction: vector (nullable = true)
     |-- probability: vector (nullable = true)
     |-- prediction: double (nullable = true)
    



```python
selected = predictions.select(col("Survived").cast("double"),col("prediction"))
```

## Step 10: evaluate the model performance


```python
predictions.show(5)
```

    +--------+------+--------+-----+------+----+-----+-------+-----+---------------+----------+---------------+---------+-------------+--------------------+--------------------+--------------------+----------+
    |Survived|   Sex|Embarked|Parch|Pclass| Age|SibSp|   Fare|AgeNA|indexedSurvived|indexedSex|indexedEmbarked|   sexVec|  embarkedVec|            features|       rawPrediction|         probability|prediction|
    +--------+------+--------+-----+------+----+-----+-------+-----+---------------+----------+---------------+---------+-------------+--------------------+--------------------+--------------------+----------+
    |     0.0|female|       C|  0.0|   3.0|29.7|  1.0|14.4542|    1|            0.0|       1.0|            1.0|(1,[],[])|(2,[1],[1.0])|[0.0,0.0,1.0,0.0,...|[-0.7732442944415...|[0.31577771844406...|       1.0|
    |     0.0|female|       C|  0.0|   3.0|29.7|  1.0|14.4583|    1|            0.0|       1.0|            1.0|(1,[],[])|(2,[1],[1.0])|[0.0,0.0,1.0,0.0,...|[-0.7733033914297...|[0.31576494996067...|       1.0|
    |     0.0|female|       C|  1.0|   3.0|18.0|  0.0|14.4542|    0|            0.0|       1.0|            1.0|(1,[],[])|(2,[1],[1.0])|[0.0,0.0,1.0,1.0,...|[-1.5102891888670...|[0.18089593933195...|       1.0|
    |     0.0|female|       C|  1.0|   3.0|45.0|  0.0|14.4542|    0|            0.0|       1.0|            1.0|(1,[],[])|(2,[1],[1.0])|[0.0,0.0,1.0,1.0,...|[-0.4426642224319...|[0.39110632180397...|       1.0|
    |     0.0|female|       C|  2.0|   3.0|29.7|  0.0|15.2458|    1|            0.0|       1.0|            1.0|(1,[],[])|(2,[1],[1.0])|[0.0,0.0,1.0,2.0,...|[-1.1484582288282...|[0.24077080651554...|       1.0|
    +--------+------+--------+-----+------+----+-----+-------+-----+---------------+----------+---------------+---------+-------------+--------------------+--------------------+--------------------+----------+
    only showing top 5 rows
    



```python
evaluator = BinaryClassificationEvaluator().setLabelCol("indexedSurvived").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
```


```python
#View model's predictions and probabilities of each prediction class
#Get the AUC
evaluator.evaluate(predictions)
```


    

    Py4JJavaErrorTraceback (most recent call last)

    <ipython-input-278-232315535e08> in <module>()
          1 #View model's predictions and probabilities of each prediction class
    ----> 2 evaluator.evaluate(predictions)
    

    /usr/lib/spark/python/pyspark/ml/evaluation.py in evaluate(self, dataset, params)
         67                 return self.copy(params)._evaluate(dataset)
         68             else:
    ---> 69                 return self._evaluate(dataset)
         70         else:
         71             raise ValueError("Params must be a param map but got %s." % type(params))


    /usr/lib/spark/python/pyspark/ml/evaluation.py in _evaluate(self, dataset)
         97         """
         98         self._transfer_params_to_java()
    ---> 99         return self._java_obj.evaluate(dataset._jdf)
        100 
        101     def isLargerBetter(self):


    /usr/lib/spark/python/lib/py4j-0.10.4-src.zip/py4j/java_gateway.py in __call__(self, *args)
       1131         answer = self.gateway_client.send_command(command)
       1132         return_value = get_return_value(
    -> 1133             answer, self.gateway_client, self.target_id, self.name)
       1134 
       1135         for temp_arg in temp_args:


    /usr/lib/spark/python/pyspark/sql/utils.py in deco(*a, **kw)
         61     def deco(*a, **kw):
         62         try:
    ---> 63             return f(*a, **kw)
         64         except py4j.protocol.Py4JJavaError as e:
         65             s = e.java_exception.toString()


    /usr/lib/spark/python/lib/py4j-0.10.4-src.zip/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
        317                 raise Py4JJavaError(
        318                     "An error occurred while calling {0}{1}{2}.\n".
    --> 319                     format(target_id, ".", name), value)
        320             else:
        321                 raise Py4JError(


    Py4JJavaError: An error occurred while calling o2652.evaluate.
    : org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 172.0 failed 4 times, most recent failure: Lost task 0.3 in stage 172.0 (TID 202, ip-172-31-37-175.ec2.internal, executor 46): org.apache.spark.SparkException: Failed to execute user defined function($anonfun$5: (string) => double)
    	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)
    	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
    	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:395)
    	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)
    	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)
    	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)
    	at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:191)
    	at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
    	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:96)
    	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:53)
    	at org.apache.spark.scheduler.Task.run(Task.scala:108)
    	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:338)
    	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    	at java.lang.Thread.run(Thread.java:748)
    Caused by: org.apache.spark.SparkException: StringIndexer encountered NULL value. To handle or skip NULLS, try setting StringIndexer.handleInvalid.
    	at org.apache.spark.ml.feature.StringIndexerModel$$anonfun$5.apply(StringIndexer.scala:213)
    	at org.apache.spark.ml.feature.StringIndexerModel$$anonfun$5.apply(StringIndexer.scala:208)
    	... 15 more
    
    Driver stacktrace:
    	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:1708)
    	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1696)
    	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1695)
    	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
    	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)
    	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:1695)
    	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:855)
    	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:855)
    	at scala.Option.foreach(Option.scala:257)
    	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:855)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:1923)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1878)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1867)
    	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:48)
    	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:671)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2029)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2050)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2069)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2094)
    	at org.apache.spark.rdd.RDD$$anonfun$collect$1.apply(RDD.scala:936)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    	at org.apache.spark.rdd.RDD.withScope(RDD.scala:362)
    	at org.apache.spark.rdd.RDD.collect(RDD.scala:935)
    	at org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.x$4$lzycompute(BinaryClassificationMetrics.scala:192)
    	at org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.x$4(BinaryClassificationMetrics.scala:146)
    	at org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.confusions$lzycompute(BinaryClassificationMetrics.scala:148)
    	at org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.confusions(BinaryClassificationMetrics.scala:148)
    	at org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.createCurve(BinaryClassificationMetrics.scala:223)
    	at org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.roc(BinaryClassificationMetrics.scala:86)
    	at org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.areaUnderROC(BinaryClassificationMetrics.scala:97)
    	at org.apache.spark.ml.evaluation.BinaryClassificationEvaluator.evaluate(BinaryClassificationEvaluator.scala:87)
    	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    	at java.lang.reflect.Method.invoke(Method.java:498)
    	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
    	at py4j.Gateway.invoke(Gateway.java:280)
    	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    	at py4j.commands.CallCommand.execute(CallCommand.java:79)
    	at py4j.GatewayConnection.run(GatewayConnection.java:214)
    	at java.lang.Thread.run(Thread.java:748)
    Caused by: org.apache.spark.SparkException: Failed to execute user defined function($anonfun$5: (string) => double)
    	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)
    	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
    	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:395)
    	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)
    	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)
    	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)
    	at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:191)
    	at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
    	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:96)
    	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:53)
    	at org.apache.spark.scheduler.Task.run(Task.scala:108)
    	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:338)
    	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    	... 1 more
    Caused by: org.apache.spark.SparkException: StringIndexer encountered NULL value. To handle or skip NULLS, try setting StringIndexer.handleInvalid.
    	at org.apache.spark.ml.feature.StringIndexerModel$$anonfun$5.apply(StringIndexer.scala:213)
    	at org.apache.spark.ml.feature.StringIndexerModel$$anonfun$5.apply(StringIndexer.scala:208)
    	... 15 more



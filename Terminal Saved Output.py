Last login: Thu Mar  1 13:10:04 on ttys000
athenas-mbp:~ athena$ cd desktop/ML2
athenas-mbp:ML2 athena$ chmod 600 AthenaMSBA.pem
athenas-mbp:ML2 athena$ ssh -i AthenaMSBA.pem hadoop@ec2-35-168-111-5.compute-1.amazonaws.com
The authenticity of host 'ec2-35-168-111-5.compute-1.amazonaws.com (35.168.111.5)' can't be established.
ECDSA key fingerprint is SHA256:VmWDkQTeyvRKBeElICSLdhn2Oxui9e9DUob+F/i2htw.
Are you sure you want to continue connecting (yes/no)? yes
Warning: Permanently added 'ec2-35-168-111-5.compute-1.amazonaws.com,35.168.111.5' (ECDSA) to the list of known hosts.

       __|  __|_  )
       _|  (     /   Amazon Linux AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-ami/2017.09-release-notes/
3 package(s) needed for security, out of 5 available
Run "sudo yum update" to apply all updates.
                                                                    
EEEEEEEEEEEEEEEEEEEE MMMMMMMM           MMMMMMMM RRRRRRRRRRRRRRR    
E::::::::::::::::::E M:::::::M         M:::::::M R::::::::::::::R   
EE:::::EEEEEEEEE:::E M::::::::M       M::::::::M R:::::RRRRRR:::::R 
  E::::E       EEEEE M:::::::::M     M:::::::::M RR::::R      R::::R
  E::::E             M::::::M:::M   M:::M::::::M   R:::R      R::::R
  E:::::EEEEEEEEEE   M:::::M M:::M M:::M M:::::M   R:::RRRRRR:::::R 
  E::::::::::::::E   M:::::M  M:::M:::M  M:::::M   R:::::::::::RR   
  E:::::EEEEEEEEEE   M:::::M   M:::::M   M:::::M   R:::RRRRRR::::R  
  E::::E             M:::::M    M:::M    M:::::M   R:::R      R::::R
  E::::E       EEEEE M:::::M     MMM     M:::::M   R:::R      R::::R
EE:::::EEEEEEEE::::E M:::::M             M:::::M   R:::R      R::::R
E::::::::::::::::::E M:::::M             M:::::M RR::::R      R::::R
EEEEEEEEEEEEEEEEEEEE MMMMMMM             MMMMMMM RRRRRRR      RRRRRR
                                                                    
[hadoop@ip-172-31-67-157 ~]$ aws s3 cp s3://ml2finalexam/reviews.csv ./
download: s3://ml2finalexam/reviews.csv to ./reviews.csv           
[hadoop@ip-172-31-67-157 ~]$ hdfs dfs -mkdir /user/training
[hadoop@ip-172-31-67-157 ~]$ hdfs dfs -mkdir /user/training/data
[hadoop@ip-172-31-67-157 ~]$ hdfs dfs -put reviews.csv /user/training/data
[hadoop@ip-172-31-67-157 ~]$ hdfs dfs -cat reviews.csv
cat: `reviews.csv': No such file or directory
[hadoop@ip-172-31-67-157 ~]$ hdfs dfs -ls /user/training/data
Found 1 items
-rw-r--r--   1 hadoop hadoop  300904694 2018-03-02 15:33 /user/training/data/reviews.csv
[hadoop@ip-172-31-67-157 ~]$ pyspark --packages com.databricks:spark.csv_2.11:1.5.0
Python 2.7.13 (default, Jan 31 2018, 00:17:36) 
[GCC 4.8.5 20150623 (Red Hat 4.8.5-11)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
Ivy Default Cache set to: /home/hadoop/.ivy2/cache
The jars for the packages stored in: /home/hadoop/.ivy2/jars
:: loading settings :: url = jar:file:/usr/lib/spark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
com.databricks#spark.csv_2.11 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent;1.0
	confs: [default]
:: resolution report :: resolve 951ms :: artifacts dl 1ms
	:: modules in use:
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   1   |   0   |   0   |   0   ||   0   |   0   |
	---------------------------------------------------------------------

:: problems summary ::
:::: WARNINGS
		module not found: com.databricks#spark.csv_2.11;1.5.0

	==== local-m2-cache: tried

	  file:/home/hadoop/.m2/repository/com/databricks/spark.csv_2.11/1.5.0/spark.csv_2.11-1.5.0.pom

	  -- artifact com.databricks#spark.csv_2.11;1.5.0!spark.csv_2.11.jar:

	  file:/home/hadoop/.m2/repository/com/databricks/spark.csv_2.11/1.5.0/spark.csv_2.11-1.5.0.jar

	==== local-ivy-cache: tried

	  /home/hadoop/.ivy2/local/com.databricks/spark.csv_2.11/1.5.0/ivys/ivy.xml

	  -- artifact com.databricks#spark.csv_2.11;1.5.0!spark.csv_2.11.jar:

	  /home/hadoop/.ivy2/local/com.databricks/spark.csv_2.11/1.5.0/jars/spark.csv_2.11.jar

	==== central: tried

	  https://repo1.maven.org/maven2/com/databricks/spark.csv_2.11/1.5.0/spark.csv_2.11-1.5.0.pom

	  -- artifact com.databricks#spark.csv_2.11;1.5.0!spark.csv_2.11.jar:

	  https://repo1.maven.org/maven2/com/databricks/spark.csv_2.11/1.5.0/spark.csv_2.11-1.5.0.jar

	==== spark-packages: tried

	  http://dl.bintray.com/spark-packages/maven/com/databricks/spark.csv_2.11/1.5.0/spark.csv_2.11-1.5.0.pom

	  -- artifact com.databricks#spark.csv_2.11;1.5.0!spark.csv_2.11.jar:

	  http://dl.bintray.com/spark-packages/maven/com/databricks/spark.csv_2.11/1.5.0/spark.csv_2.11-1.5.0.jar

		::::::::::::::::::::::::::::::::::::::::::::::

		::          UNRESOLVED DEPENDENCIES         ::

		::::::::::::::::::::::::::::::::::::::::::::::

		:: com.databricks#spark.csv_2.11;1.5.0: not found

		::::::::::::::::::::::::::::::::::::::::::::::



:: USE VERBOSE OR DEBUG MESSAGE LEVEL FOR MORE DETAILS
Exception in thread "main" java.lang.RuntimeException: [unresolved dependency: com.databricks#spark.csv_2.11;1.5.0: not found]
	at org.apache.spark.deploy.SparkSubmitUtils$.resolveMavenCoordinates(SparkSubmit.scala:1197)
	at org.apache.spark.deploy.SparkSubmit$.prepareSubmitEnvironment(SparkSubmit.scala:304)
	at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:153)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:119)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Traceback (most recent call last):
  File "/usr/lib/spark/python/pyspark/shell.py", line 38, in <module>
    SparkContext._ensure_initialized()
  File "/usr/lib/spark/python/pyspark/context.py", line 283, in _ensure_initialized
    SparkContext._gateway = gateway or launch_gateway(conf)
  File "/usr/lib/spark/python/pyspark/java_gateway.py", line 95, in launch_gateway
    raise Exception("Java gateway process exited before sending the driver its port number")
Exception: Java gateway process exited before sending the driver its port number
>>> exit()
[hadoop@ip-172-31-67-157 ~]$ pyspark --packages com.databricks:spark-csv_2.11:1.5.0
Python 2.7.13 (default, Jan 31 2018, 00:17:36) 
[GCC 4.8.5 20150623 (Red Hat 4.8.5-11)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
Ivy Default Cache set to: /home/hadoop/.ivy2/cache
The jars for the packages stored in: /home/hadoop/.ivy2/jars
:: loading settings :: url = jar:file:/usr/lib/spark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
com.databricks#spark-csv_2.11 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent;1.0
	confs: [default]
	found com.databricks#spark-csv_2.11;1.5.0 in central
	found org.apache.commons#commons-csv;1.1 in central
	found com.univocity#univocity-parsers;1.5.1 in central
downloading https://repo1.maven.org/maven2/com/databricks/spark-csv_2.11/1.5.0/spark-csv_2.11-1.5.0.jar ...
	[SUCCESSFUL ] com.databricks#spark-csv_2.11;1.5.0!spark-csv_2.11.jar (31ms)
downloading https://repo1.maven.org/maven2/org/apache/commons/commons-csv/1.1/commons-csv-1.1.jar ...
	[SUCCESSFUL ] org.apache.commons#commons-csv;1.1!commons-csv.jar (22ms)
downloading https://repo1.maven.org/maven2/com/univocity/univocity-parsers/1.5.1/univocity-parsers-1.5.1.jar ...
	[SUCCESSFUL ] com.univocity#univocity-parsers;1.5.1!univocity-parsers.jar (11ms)
:: resolution report :: resolve 1190ms :: artifacts dl 71ms
	:: modules in use:
	com.databricks#spark-csv_2.11;1.5.0 from central in [default]
	com.univocity#univocity-parsers;1.5.1 from central in [default]
	org.apache.commons#commons-csv;1.1 from central in [default]
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   3   |   3   |   3   |   0   ||   3   |   3   |
	---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent
	confs: [default]
	3 artifacts copied, 0 already retrieved (344kB/13ms)
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
18/03/02 15:41:52 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
18/03/02 15:41:56 WARN Client: Same path resource file:/home/hadoop/.ivy2/jars/com.databricks_spark-csv_2.11-1.5.0.jar added multiple times to distributed cache.
18/03/02 15:41:56 WARN Client: Same path resource file:/home/hadoop/.ivy2/jars/org.apache.commons_commons-csv-1.1.jar added multiple times to distributed cache.
18/03/02 15:41:56 WARN Client: Same path resource file:/home/hadoop/.ivy2/jars/com.univocity_univocity-parsers-1.5.1.jar added multiple times to distributed cache.
18/03/02 15:42:20 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 1.2.0
18/03/02 15:42:21 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException
18/03/02 15:42:21 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.2.1
      /_/

Using Python version 2.7.13 (default, Jan 31 2018 00:17:36)
SparkSession available as 'spark'.
>>> sqlContext = SQLContext(sc)
>>> rawdata = sqlContext.read.format('com.databricks.spark.csv').option('inferSchema', True).option("header".True).load('hdfs:///user/training/data/reviews.csv')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
AttributeError: 'str' object has no attribute 'True'
>>> rawdata = sqlContext.read.format('com.databricks.spark.csv').option('inferSchema', True).option("header",True).load('hdfs:///user/training/data/reviews.csv')
>>> 18/03/02 16:01:06 WARN Errors: The following warnings have been detected: WARNING: The (sub)resource method stageData in org.apache.spark.status.api.v1.OneStageResource contains empty path annotation.
    rawdata.show(5)
+---+----------+--------------+--------------------+--------------------+----------------------+-----+----------+--------------------+--------------------+
| Id| ProductId|        UserId|         ProfileName|HelpfulnessNumerator|HelpfulnessDenominator|Score|      Time|             Summary|                Text|
+---+----------+--------------+--------------------+--------------------+----------------------+-----+----------+--------------------+--------------------+
|  1|B001E4KFG0|A3SGXH7AUHU8GW|          delmartian|                   1|                     1|    5|1303862400|Good Quality Dog ...|I have bought sev...|
|  2|B00813GRG4|A1D87F6ZCVE5NK|              dll pa|                   0|                     0|    1|1346976000|   Not as Advertised|"Product arrived ...|
|  3|B000LQOCH0| ABXLMWJIXXAIN|"Natalia Corres "...|                   1|                     1|    4|1219017600|"""Delight"" says...|"This is a confec...|
|  4|B000UA0QIQ|A395BORC6FGVXV|                Karl|                   3|                     3|    2|1307923200|      Cough Medicine|If you are lookin...|
|  5|B006K2ZZ7K|A1UQRSCLF8GW1T|"Michael D. Bigha...|                   0|                     0|    5|1350777600|         Great taffy|Great taffy at a ...|
+---+----------+--------------+--------------------+--------------------+----------------------+-----+----------+--------------------+--------------------+
only showing top 5 rows

>>> from pyspark.llib.feature import HashingTF
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ImportError: No module named llib.feature
>>> from pyspark.Mllib.feature import HashingTF
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ImportError: No module named Mllib.feature
>>> from pyspark.mllib.feature import HashingTF
>>> from pyspark import SparkContext
>>> rawdata.printSchema()
root
 |-- Id: integer (nullable = true)
 |-- ProductId: string (nullable = true)
 |-- UserId: string (nullable = true)
 |-- ProfileName: string (nullable = true)
 |-- HelpfulnessNumerator: string (nullable = true)
 |-- HelpfulnessDenominator: string (nullable = true)
 |-- Score: string (nullable = true)
 |-- Time: string (nullable = true)
 |-- Summary: string (nullable = true)
 |-- Text: string (nullable = true)

>>> tf = hashingTF.transform(rawdata)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'hashingTF' is not defined
>>> hashingTF = HashingTF()
>>> tf = hashingTF.transform(rawdata)
>>> tf.cache()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
AttributeError: 'SparseVector' object has no attribute 'cache'
>>> idf = IDF().fit(tf)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'IDF' is not defined
>>> from pyspark.mllib.feature import IDF
>>> idf = IDF().fit(tf)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/lib/spark/python/pyspark/mllib/feature.py", line 572, in fit
    raise TypeError("dataset should be an RDD of term frequency vectors")
TypeError: dataset should be an RDD of term frequency vectors
>>> rawdata.map(lambda line: line.split(" "))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/lib/spark/python/pyspark/sql/dataframe.py", line 1020, in __getattr__
    "'%s' object has no attribute '%s'" % (self.__class__.__name__, name))
AttributeError: 'DataFrame' object has no attribute 'map'
>>> tokenizer = Tokenizer(inputCol="Summary", outputCol="words")
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'Tokenizer' is not defined
>>> from pyspark.ml.feature import Tokenizer, RegexTokenizer
>>> tokenizer = Tokenizer(inputCol="Summary", outputCol="words")
>>> df_tokens = tokenizer.transform(rawdata)
>>> from pyspark.ml.feature import StopWordsRemover
>>> remover = StopWordsRemover(inputCol="words", outputCol="filtered")
>>> remover.transform(df_tokens).show(truncate=False)
+---+----------+--------------+-----------------------------------+--------------------+----------------------+-----+----------+----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+---------------------------------------------------------+
|Id |ProductId |UserId        |ProfileName                        |HelpfulnessNumerator|HelpfulnessDenominator|Score|Time      |Summary                                                         |Text                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |words                                                                       |filtered                                                 |
+---+----------+--------------+-----------------------------------+--------------------+----------------------+-----+----------+----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+---------------------------------------------------------+
|1  |B001E4KFG0|A3SGXH7AUHU8GW|delmartian                         |1                   |1                     |5    |1303862400|Good Quality Dog Food                                           |I have bought several of the Vitality canned dog food products and have found them all to be of good quality. The product looks more like a stew than a processed meat and it smells better. My Labrador is finicky and she appreciates this product better than  most.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |[good, quality, dog, food]                                                  |[good, quality, dog, food]                               |
|2  |B00813GRG4|A1D87F6ZCVE5NK|dll pa                             |0                   |0                     |1    |1346976000|Not as Advertised                                               |"Product arrived labeled as Jumbo Salted Peanuts...the peanuts were actually small sized unsalted. Not sure if this was an error or if the vendor intended to represent the product as ""Jumbo""."                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |[not, as, advertised]                                                       |[advertised]                                             |
|3  |B000LQOCH0|ABXLMWJIXXAIN |"Natalia Corres ""Natalia Corres"""|1                   |1                     |4    |1219017600|"""Delight"" says it all"                                       |"This is a confection that has been around a few centuries.  It is a light, pillowy citrus gelatin with nuts - in this case Filberts. And it is cut into tiny squares and then liberally coated with powdered sugar.  And it is a tiny mouthful of heaven.  Not too chewy, and very flavorful.  I highly recommend this yummy treat.  If you are familiar with the story of C.S. Lewis' ""The Lion                                                                                                                                                                                                                                                                                                                                                                                                         |["""delight"", says, it, all"]                                              |["""delight"", says, all"]                               |
|4  |B000UA0QIQ|A395BORC6FGVXV|Karl                               |3                   |3                     |2    |1307923200|Cough Medicine                                                  |If you are looking for the secret ingredient in Robitussin I believe I have found it.  I got this in addition to the Root Beer Extract I ordered (which was good) and made some cherry soda.  The flavor is very medicinal.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |[cough, medicine]                                                           |[cough, medicine]                                        |
|5  |B006K2ZZ7K|A1UQRSCLF8GW1T|"Michael D. Bigham ""M. Wassir"""  |0                   |0                     |5    |1350777600|Great taffy                                                     |Great taffy at a great price.  There was a wide assortment of yummy taffy.  Delivery was very quick.  If your a taffy lover, this is a deal.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |[great, taffy]                                                              |[great, taffy]                                           |
|6  |B006K2ZZ7K|ADT0SRK1MGOEU |Twoapennything                     |0                   |0                     |4    |1342051200|Nice Taffy                                                      |I got a wild hair for taffy and ordered this five pound bag. The taffy was all very enjoyable with many flavors: watermelon, root beer, melon, peppermint, grape, etc. My only complaint is there was a bit too much red/black licorice-flavored pieces (just not my particular favorites). Between me, my kids, and my husband, this lasted only two weeks! I would recommend this brand of taffy -- it was a delightful treat.                                                                                                                                                                                                                                                                                                                                                                           |[nice, taffy]                                                               |[nice, taffy]                                            |
|7  |B006K2ZZ7K|A1SP2KVKFXXRU1|David C. Sullivan                  |0                   |0                     |5    |1340150400|Great!  Just as good as the expensive brands!                   |This saltwater taffy had great flavors and was very soft and chewy.  Each candy was individually wrapped well.  None of the candies were stuck together, which did happen in the expensive version, Fralinger's.  Would highly recommend this candy!  I served it at a beach-themed party and everyone loved it!                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |[great!, , just, as, good, as, the, expensive, brands!]                     |[great!, , good, expensive, brands!]                     |
|8  |B006K2ZZ7K|A3JRGQVEQN31IQ|Pamela G. Williams                 |0                   |0                     |5    |1336003200|Wonderful, tasty taffy                                          |This taffy is so good.  It is very soft and chewy.  The flavors are amazing.  I would definitely recommend you buying it.  Very satisfying!!                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |[wonderful,, tasty, taffy]                                                  |[wonderful,, tasty, taffy]                               |
|9  |B000E7L2R4|A1MZYO9TZK0BBI|R. James                           |1                   |1                     |5    |1322006400|Yay Barley                                                      |Right now I'm mostly just sprouting this so my cats can eat the grass. They love it. I rotate it around with Wheatgrass and Rye too                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |[yay, barley]                                                               |[yay, barley]                                            |
|10 |B00171APVA|A21BT40VZCCYT4|Carol A. Reed                      |0                   |0                     |5    |1351209600|Healthy Dog Food                                                |This is a very healthy dog food. Good for their digestion. Also good for small puppies. My dog eats her required amount at every feeding.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |[healthy, dog, food]                                                        |[healthy, dog, food]                                     |
|11 |B0001PB9FE|A3HDKO7OW0QNK4|Canadian Fan                       |1                   |1                     |5    |1107820800|The Best Hot Sauce in the World                                 |I don't know if it's the cactus or the tequila or just the unique combination of ingredients, but the flavour of this hot sauce makes it one of a kind!  We picked up a bottle once on a trip we were on and brought it back home with us and were totally blown away!  When we realized that we simply couldn't find it anywhere in our city we were bummed.<br /><br />Now, because of the magic of the internet, we have a case of the sauce and are ecstatic because of it.<br /><br />If you love hot sauce..I mean really love hot sauce, but don't want a sauce that tastelessly burns your throat, grab a bottle of Tequila Picante Gourmet de Inclan.  Just realize that once you taste it, you will never want to use any other sauce.<br /><br />Thank you for the personal, incredible service!|[the, best, hot, sauce, in, the, world]                                     |[best, hot, sauce, world]                                |
|12 |B0009XLVG0|A2725IB4YY9JEB|"A Poeng ""SparkyGoHome"""         |4                   |4                     |5    |1282867200|"My cats LOVE this ""diet"" food better than their regular food"|One of my boys needed to lose some weight and the other didn't.  I put this food on the floor for the chubby guy, and the protein-rich, no by-product food up higher where only my skinny boy can jump.  The higher food sits going stale.  They both really go for this food.  And my chubby boy has been losing about an ounce a week.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |["my, cats, love, this, ""diet"", food, better, than, their, regular, food"]|["my, cats, love, ""diet"", food, better, regular, food"]|
|13 |B0009XLVG0|A327PCT23YH90 |LT                                 |1                   |1                     |1    |1339545600|My Cats Are Not Fans of the New Food                            |My cats have been happily eating Felidae Platinum for more than two years. I just got a new bag and the shape of the food is different. They tried the new food when I first put it in their bowls and now the bowls sit full and the kitties will not touch the food. I've noticed similar reviews related to formula changes in the past. Unfortunately, I now need to find a new food that my cats will eat.                                                                                                                                                                                                                                                                                                                                                                                            |[my, cats, are, not, fans, of, the, new, food]                              |[cats, fans, new, food]                                  |
|14 |B001GVISJM|A18ECVX2RJ7HUE|"willie ""roadie"""                |2                   |2                     |4    |1288915200|fresh and greasy!                                               |good flavor! these came securely packed... they were fresh and delicious! i love these Twizzlers!                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |[fresh, and, greasy!]                                                       |[fresh, greasy!]                                         |
|15 |B001GVISJM|A2MUGFV2TDQ47K|"Lynrie ""Oh HELL no"""            |4                   |5                     |5    |1268352000|Strawberry Twizzlers - Yummy                                    |The Strawberry Twizzlers are my guilty pleasure - yummy. Six pounds will be around for a while with my son and I.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |[strawberry, twizzlers, -, yummy]                                           |[strawberry, twizzlers, -, yummy]                        |
|16 |B001GVISJM|A1CZX3CP8IKQIJ|Brian A. Lee                       |4                   |5                     |5    |1262044800|Lots of twizzlers, just what you expect.                        |My daughter loves twizzlers and this shipment of six pounds really hit the spot. It's exactly what you would expect...six packages of strawberry twizzlers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |[lots, of, twizzlers,, just, what, you, expect.]                            |[lots, twizzlers,, expect.]                              |
|17 |B001GVISJM|A3KLWF6WQ5BNYO|Erica Neathery                     |0                   |0                     |2    |1348099200|poor taste                                                      |I love eating them and they are good for watching TV and looking at movies! It is not too sweet. I like to transfer them to a zip lock baggie so they stay fresh so I can take my time eating them.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |[poor, taste]                                                               |[poor, taste]                                            |
|18 |B001GVISJM|AFKW14U97Z6QO |Becca                              |0                   |0                     |5    |1345075200|Love it!                                                        |I am very satisfied with my Twizzler purchase.  I shared these with others and we have all enjoyed them.  I will definitely be ordering more.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |[love, it!]                                                                 |[love, it!]                                              |
|19 |B001GVISJM|A2A9X58G2GTBLP|Wolfee1                            |0                   |0                     |5    |1324598400|GREAT SWEET CANDY!                                              |Twizzlers, Strawberry my childhood favorite candy, made in Lancaster Pennsylvania by Y & S Candies, Inc. one of the oldest confectionery Firms in the United States, now a Subsidiary of the Hershey Company, the Company was established in 1845 as Young and Smylie, they also make Apple Licorice Twists, Green Color and Blue Raspberry Licorice Twists, I like them all<br /><br />I keep it in a dry cool place because is not recommended it to put it in the fridge. According to the Guinness Book of Records, the longest Licorice Twist ever made measured 1.200 Feet (370 M) and weighted 100 Pounds (45 Kg) and was made by Y & S Candies, Inc. This Record-Breaking Twist became a Guinness World Record on July 19, 1998. This Product is Kosher! Thank You                                 |[great, sweet, candy!]                                                      |[great, sweet, candy!]                                   |
|20 |B001GVISJM|A3IV7CL2C13K2U|Greg                               |0                   |0                     |5    |1318032000|Home delivered twizlers                                         |Candy was delivered very fast and was purchased at a reasonable price.  I was home bound and unable to get to a store so this was perfect for me.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |[home, delivered, twizlers]                                                 |[home, delivered, twizlers]                              |
+---+----------+--------------+-----------------------------------+--------------------+----------------------+-----+----------+----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+---------------------------------------------------------+
only showing top 20 rows

>>> idf = IDF().fit(new_df)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'new_df' is not defined
>>> new_df = remover.transform(df_tokens)
>>> idf = IDF().fit(new_df)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/lib/spark/python/pyspark/mllib/feature.py", line 572, in fit
    raise TypeError("dataset should be an RDD of term frequency vectors")
TypeError: dataset should be an RDD of term frequency vectors
>>> hashingTF = HashingTF(inputCol = "words", outputCol = "rawfeatures", numFeatures = 20).transform(new_df)
idf = IDF().fit(hashingTF)
tfidf = idf.transform(hashingTF)
tfidf.collect()
tfidf.cache()
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}

val lda = new LDA()
  .setK(10)
  .setMaxIter(10)
  .setFeaturesCol(FEATURES_COL)
val model = lda.fit()
val transformed = model.transform(tfidf)

val ll = model.logLikelihood(tfidf)
val lp = model.logPerplexity(tfidf)

// describeTopics
val topics = model.describeTopics(10)

// Shows the result
topics.show(true)
transformed.show(true)

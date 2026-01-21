# Databricks notebook source
#imports
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Reading(_sensor_json
# MAGIC %fs ls /Volumes/dev/club_db/data/json/sensor_data_Json.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Sensor Data Ingested from JSON File (Structured JSON)

# COMMAND ----------

# DBTITLE 1,Cell 3
df_sensors=spark.read.option("multiline", "true").json("/Volumes/dev/club_db/data/json/sensor_data_Json.json")
display(df_sensors)
#df_sensors.printSchema()


# COMMAND ----------

df_sensors.select("dc_id", explode("source")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### User-Provided Data (JSON Stored as String, Parsed Using get_json_object)
# MAGIC

# COMMAND ----------

user_data = [
    (1, """{
        "gender": "female",
        "name": {
            "title": "ms",
            "first": "inmaculada",
            "last": "lozano"
        },
        "location": {
            "street": "1516 avenida de la albufera",
            "city": "mérida",
            "state": "cantabria",
            "zip": 95196
        },
        "email": "inmaculada.lozano@example.com",
        "username": "organicladybug660",
        "password": "californ",
        "salt": "6QKT8fXY",
        "md5": "890d04290d02027812610dba075802d8",
        "sha1": "dfca140a17c86f8f0bd82e624369ccb962f37286",
        "sha256": "51b7e09a415fef7e71eb3aec2721e91d0b51c2e264a7cdd41f3a692777771de3",
        "registered": 1015289589,
        "dob": 189756976,
        "phone": "925-063-227",
        "cell": "656-640-785",
        "DNI": "68030488-B",
        "picture": {
            "large": "https://randomuser.me/api/portraits/women/54.jpg",
            "medium": "https://randomuser.me/api/portraits/med/women/54.jpg",
            "thumbnail": "https://randomuser.me/api/portraits/thumb/women/54.jpg"
        }
    }"""),
    (2, """{
        "gender": "male",
        "name": {
            "title": "mr",
            "first": "german",
            "last": "campos"
        },
        "location": {
            "street": "7820 calle del prado",
            "city": "pontevedra",
            "state": "comunidad valenciana",
            "zip": 17556
        },
        "email": "german.campos@example.com",
        "username": "redswan557",
        "password": "boobs",
        "salt": "jgu0hG4z",
        "md5": "06d4fe8701eeea514b06997bdfcfa831",
        "sha1": "048ac6c49ea448e23e63bdaa8e69172a210710cf",
        "sha256": "ff3564e2d12d7c6e21feb7f2f6f88ea0d9f91308e1b0f00aa3fd2fe1dc163a65",
        "registered": 1311909298,
        "dob": 1108525374,
        "phone": "926-930-644",
        "cell": "669-260-270",
        "DNI": "23880337-O",
        "picture": {
            "large": "https://randomuser.me/api/portraits/men/50.jpg",
            "medium": "https://randomuser.me/api/portraits/med/men/50.jpg",
            "thumbnail": "https://randomuser.me/api/portraits/thumb/men/50.jpg"
        }
    }"""),
	(3,"""{
        "gender": "female",
        "name": {
            "title": "mrs",
            "first": "magdalena",
            "last": "velasco"
        },
        "location": {
            "street": "5470 calle de arganzuela",
            "city": "vigo",
            "state": "comunidad valenciana",
            "zip": 15638
        },
        "email": "magdalena.velasco@example.com",
        "username": "tinymouse931",
        "password": "daisy",
        "salt": "l36WnDxT",
        "md5": "c11736a48b73b333bd6803f6555c4efe",
        "sha1": "37d244cea04902f5c71ff51a8e80861c848db2ae",
        "sha256": "71a27886f41bdd81e647e2bc151e7821a65843fe5c83f33f96bdcfa94d35b429",
        "registered": 1390466214,
        "dob": 482250439,
        "phone": "979-295-265",
        "cell": "684-933-245",
        "DNI": "94134545-L",
        "picture": {
            "large": "https://randomuser.me/api/portraits/women/1.jpg",
            "medium": "https://randomuser.me/api/portraits/med/women/1.jpg",
            "thumbnail": "https://randomuser.me/api/portraits/thumb/women/1.jpg"
        }
    }"""),
	(4,"""{
        "gender": "female",
        "name": {
            "title": "mrs",
            "first": "luisa",
            "last": "mendez"
        },
        "location": {
            "street": "4631 avenida de castilla",
            "city": "logroño",
            "state": "andalucía",
            "zip": 22515
        },
        "email": "luisa.mendez@example.com",
        "username": "heavybutterfly513",
        "password": "qwerty",
        "salt": "qWN3fcpw",
        "md5": "4ac6ba701a5c04a883a3ffa1c7cc8a7c",
        "sha1": "72ec49b74ac7238b9e2c680289930f6907977c84",
        "sha256": "ce308c5e6a49482c6a0c8587fd26757839c24e99f439c33781578e1d47feab55",
        "registered": 1055859371,
        "dob": 425508875,
        "phone": "957-552-159",
        "cell": "609-373-141",
        "DNI": "46561148-S",
        "picture": {
            "large": "https://randomuser.me/api/portraits/women/67.jpg",
            "medium": "https://randomuser.me/api/portraits/med/women/67.jpg",
            "thumbnail": "https://randomuser.me/api/portraits/thumb/women/67.jpg"
        }
    }""")
]

user_json_df = spark.createDataFrame(user_data, ["user_id", "user_details"])
display(user_json_df)
#user_json_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Task prepare the report from user_data which should have user_id,name,gender

# COMMAND ----------

# DBTITLE 1,Cell 8
#get_json_object(expr,path)
user_json_df.select("user_id", get_json_object("user_details", "$.name").alias("name"),
                               get_json_object("user_details", "$.name.first").alias("frist"),
                               get_json_object("user_details", "$.name.last").alias("last"),
                               get_json_object("user_details", "$.gender").alias("gender"),).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Creating the schema using from_json mapping 
# MAGIC ####### Defining Schema and Parsing User JSON String Using from_json()
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType

user_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("name", StructType([
        StructField("title", StringType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True)
    ]), True),
    StructField("location", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", LongType(), True)
    ]), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("password", StringType(), True),
    StructField("salt", StringType(), True),
    StructField("md5", StringType(), True),
    StructField("sha1", StringType(), True),
    StructField("sha256", StringType(), True),
    StructField("registered", LongType(), True),
    StructField("dob", LongType(), True),
    StructField("phone", StringType(), True),
    StructField("cell", StringType(), True),
    StructField("DNI", StringType(), True),
    StructField("picture", StructType([
        StructField("large", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("thumbnail", StringType(), True)
    ]), True)
])


# COMMAND ----------

from pyspark.sql.functions import from_json, col

parsed_df = user_json_df.withColumn(
    "user",
    from_json(col("user_details"), user_schema)
)

display(parsed_df)


# COMMAND ----------

parsed_df.select(
    "user_id",
    "user.gender",
    "user.name.first",
    "user.location.city",
    "user.email"
).show(truncate=False)


# COMMAND ----------



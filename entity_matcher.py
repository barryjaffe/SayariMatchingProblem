#spark-submit --master "local[*]" entity_matcher.py

from pyspark.sql import SparkSession
import sys

print('cmd entry:', sys.argv)

uk_file_path = "gbr.jsonl.gz" # sys.argv[1]
ofac_file_path = "ofac.jsonl.gz" # sys.argv[2]
output_path = "entity_matches.tsv" # sys.argv[3]

ofac_prefix = "ofac"
gbr_prefix = "gbr"

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def extract_entities(session, source):
    df = spark.sql(f"SELECT * FROM {source} WHERE type = 'Entity'")
    df.createOrReplaceTempView(f"{source}_entity")
    return df

def exploded_data(session, source):
    df = session.sql(f"""SELECT
     CAST(ltrim(id) as long)                 id,
     lower(trim(name))                       name,
     lower(trim(alias.value))                alias_value,
     lower(trim(alias.type))                 alias_type,
     lower(trim(id_number.value))            id_number_value,
     lower(trim(id_number.comment))          id_number_comment,
     lower(trim(position))                   position,
     lower(trim(place_of_birth))             place_of_birth,
     lower(trim(exploded_addresses.country)) address_country
    FROM {source}_entity
    LATERAL VIEW OUTER explode(id_numbers) exploded_id_numbers as id_number
    LATERAL VIEW OUTER explode(array_distinct(flatten(array(aliases, array(struct('name' as type, name as value)))))) exploded_aliases as alias
    LATERAL VIEW OUTER explode(array_distinct(addresses)) exploded_addresses as exploded_addresses
    ORDER BY id""")
    df.createOrReplaceTempView(f"{source}_entity_data")
    return df


spark = SparkSession.builder \
    .appName("OFAC/GBR Entity Matching") \
    .getOrCreate() \

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print(f"Spark version = {spark.version}")
print(f"Hadoop version = {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

gbrDF = spark.read.json(uk_file_path)
gbrDF.printSchema()
gbrDF.createOrReplaceTempView(gbr_prefix)
gbrEntityDF = extract_entities(spark, gbr_prefix)

ofacDF = spark.read.json(ofac_file_path)
ofacDF.printSchema()
ofacDF.createOrReplaceTempView(ofac_prefix)
ofacEntityDF = extract_entities(spark, ofac_prefix)

gbrEntityNamesDF = exploded_data(spark, gbr_prefix)
ofacEntityNamesDF = exploded_data(spark, ofac_prefix)


matchingNamesDF = spark.sql("""SELECT DISTINCT
  o.id ofac_id,
  g.id uk_id,
  o.alias_value ofac_name,
  g.alias_value uk_name,
  o.place_of_birth ofac_place_of_birth,
  g.place_of_birth uk_place_of_birth,
  o.address_country   ofac_address_country,
  g.address_country   uk_address_country
FROM gbr_entity_data g
JOIN ofac_entity_data o ON (levenshtein(g.alias_value,o.alias_value) < 3)
WHERE nvl(levenshtein(g.address_country, o.address_country), 1) < 3
""")
matchingNamesDF.cache() # for incremental debugging
matchingNamesDF.createOrReplaceTempView("name_matches")

# Calculate matches by further restricting name matches by birth date range matching
# Rank the data by matching criteria, along with some additonal "hacky" overlap measures, taking the "best"
# value by ordering clause.  Using row_number (calling it rank), rank/dense rank may result in multiple rows
# within the same window having a rank of 1 (where row_number simply assign a monotonically icreasing value).
entityMatchesDF = spark.sql("""WITH results AS (SELECT
  m.uk_id,
  m.ofac_id,
  levenshtein(m.uk_name, m.ofac_name) name_distance,
  nvl(levenshtein(m.uk_address_country, m.ofac_address_country), 1) address_country_distance,
  size(array_intersect(transform(split(m.uk_place_of_birth, '[,]'), x -> trim(x)),
                       transform(split(m.ofac_place_of_birth, '[,]'), x -> trim(x)))) place_of_birth_overlap, 
  m.uk_name,
  m.ofac_name,
  m.uk_address_country,
  m.ofac_address_country
FROM name_matches m),
rankings as (
SELECT DISTINCT
  uk_id,
  ofac_id,
  uk_name,
  ofac_name,
  uk_address_country,
  ofac_address_country,
  name_distance,
  address_country_distance,
  ROW_NUMBER() OVER (PARTITION BY /*uk_id,*/ ofac_id ORDER BY name_distance, address_country_distance) rank
FROM results
ORDER BY ofac_id, uk_id, rank)
SELECT *
FROM rankings
WHERE rank = 1
ORDER BY uk_id, ofac_id, uk_name
""")

print("calculating results...")
entityMatchesDF.show(truncate=False)
entityMatchesDF.createOrReplaceTempView("entity_matches")
entityMatchesDF.write.mode("overwrite").option("header", True).option("delimiter", '\t').csv(output_path)
print(f"Entity match count: {entityMatchesDF.count()}")

print("duplicate detection")
ukDuplicatesDF = spark.sql("""SELECT
ofac_id, COUNT(DISTINCT uk_id) dup_count, COLLECT_SET(uk_id) dup_uk_ids
FROM entity_matches
GROUP BY ofac_id
HAVING COUNT(DISTINCT uk_id) > 1
""")
ukDuplicatesDF.show(20, False)

ofacDuplicatesDF = spark.sql("""SELECT
uk_id, COUNT(DISTINCT ofac_id) dup_count, COLLECT_SET(ofac_id) dup_ofac_ids
FROM entity_matches
GROUP BY uk_id
HAVING COUNT(DISTINCT ofac_id) > 1
""")
ofacDuplicatesDF.show(20, False)

spark.sql("show tables").show()
#spark.stop()

print("done")

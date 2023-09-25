#spark-submit --master "local[*]" individual_matcher.py

from pyspark.sql import SparkSession
import sys

print('cmd entry:', sys.argv)

uk_file_path = "gbr.jsonl.gz" # sys.argv[1]
ofac_file_path = "ofac.jsonl.gz" # sys.argv[2]
output_path = "individual_matches.tsv" # sys.argv[3]

ofac_prefix = "ofac"
gbr_prefix = "gbr"

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def extract_individuals(session, source):
    df = spark.sql(f"SELECT * FROM {source} WHERE type = 'Individual'")
    df.createOrReplaceTempView(f"{source}_individual")
    return df

def exploded_data(session, source):
    df = session.sql(f"""SELECT
     CAST(ltrim(id) as long)             id,
     lower(trim(name))                   name,
     lower(trim(alias.value))            alias_value,
     lower(trim(alias.type))             alias_type,
     lower(trim(id_number.value))        id_number_value,
     lower(trim(id_number.comment))      id_number_comment,
     lower(trim(position))               position,
     lower(trim(place_of_birth))         place_of_birth,
     lower(trim(nation_of_origin))       nationality
    FROM {source}_individual
    LATERAL VIEW OUTER explode(id_numbers) exploded_id_numbers as id_number
    LATERAL VIEW OUTER explode(array_distinct(flatten(array(aliases, array(struct('name' as type, name as value)))))) exploded_aliases as alias
    LATERAL VIEW OUTER explode(nationality) exploded_nationality as nation_of_origin
    ORDER BY id""")
    df.createOrReplaceTempView(f"{source}_individual_data")
    return df


spark = SparkSession.builder \
    .appName("OFAC/GBR Individual Matching") \
    .getOrCreate() \

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print(f"Spark version = {spark.version}")
print(f"Hadoop version = {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

gbrDF = spark.read.json(uk_file_path)
gbrDF.printSchema()
gbrDF.createOrReplaceTempView(gbr_prefix)
gbrIndividualDF = extract_individuals(spark, gbr_prefix)

ofacDF = spark.read.json(ofac_file_path)
ofacDF.printSchema()
ofacDF.createOrReplaceTempView(ofac_prefix)
ofacIndividualDF = extract_individuals(spark, ofac_prefix)

gbrIndividualNamesDF = exploded_data(spark, gbr_prefix)
ofacIndividualNamesDF = exploded_data(spark, ofac_prefix)

"""
ofac dob samples
+----+--------+----------------------------------------+----------------------------------------+
|dob |count(1)|min(lower(trim(reported_date_of_birth)))|max(lower(trim(reported_date_of_birth)))|
+----+--------+----------------------------------------+----------------------------------------+
|null|198     |null                                    |null                                    |
|4   |854     |1924                                    |1995                                    |
|8   |48      |apr 1961                                |sep 1977                                |
|10  |75      |circa 1934                              |circa 1994                              |
|11  |3945    |01 apr 1950                             |31 oct 1989                             |
|12  |56      |1929 to 1930                            |1996 to 1997                            |
|17  |2       |circa 01 jan 1961                       |circa 07 jul 1966                       |
|20  |1       |mar 1962 to feb 1963                    |mar 1962 to feb 1963                    |
|26  |55      |01 dec 1952 to 31 dec 1952              |26 sep 1946 to 07 dec 1946              |
+----+--------+----------------------------------------+----------------------------------------+

gbr dob samples
+----+--------+----------------------------------------+----------------------------------------+
|dob |count(1)|min(lower(trim(reported_date_of_birth)))|max(lower(trim(reported_date_of_birth)))|
+----+--------+----------------------------------------+----------------------------------------+
|null|285     |null                                    |null                                    |
|10  |1741    |00/00/1923                              |31/12/1979                              |
+----+--------+----------------------------------------+----------------------------------------+
"""

# gbr dates: (note: making dates the same "shape" in order to compare them)
# * single full or partial date with dd/MM/yyyy format
# * create a min/max for each date, for full date min = max, for partial date create appropriate min and max date

gbrDobDF = spark.sql(r"""
WITH date_parts AS (SELECT id, dob,
   to_date(dob, 'dd/MM/yyyy') day_date,
   to_date(regexp_extract(dob, '(\\d+/)(\\d+/\\d+)', 2), 'MM/yyyy') month_date,
   to_date(regexp_extract(dob, '(\\d+/\\d+/)(\\d+)', 2), 'yyyy') year_date
FROM gbr_individual
LATERAL VIEW explode(transform(reported_dates_of_birth, x -> lower(trim(x)))) exploded_reported_dates_of_birth as dob),
date_ranges AS (SELECT
   id,
   dob,
   day_date,
   month_date,
   year_date,
   CASE
     WHEN day_date IS NOT NULL THEN
       named_struct("min", day_date, "max", day_date)
     WHEN month_date IS NOT NULL THEN
       named_struct("min", month_date, "max", last_day(month_date))
     WHEN year_date IS NOT NULL  THEN
       named_struct("min", year_date, "max", date_sub(add_months(year_date, 12), 1))
     ELSE NULL
   END date_range
FROM date_parts)
SELECT
  id,
  dob,
  date_range.min min_dob,
  date_range.max max_dob
FROM date_ranges
""")
gbrDobDF.createOrReplaceTempView("gbr_dob")

# ofac dates: (note: making dates the same "shape" in order to compare them)
# * partial and complete dates
# * some dates specified as a range using ' to ' as separator.
# * some dates have a 'circa' modifier; current padding for circa dates is:
#   +/- 15 days for fully specified date
#   +/- 2 months for month specific date
#   +/- 12 months for year specific date
#   note: circa padding should be parameterized
ofacDobDF = spark.sql(r"""
WITH date_parts AS (
SELECT
   id,
   dob,
   dob like 'circa%' circa,
   transform(if(dob LIKE '% to %', split(dob, ' to '), array(replace(dob, 'circa ', ''))),
      x -> struct(
      to_date(x, 'dd MMM yyyy') as day_date,
      to_date(x, 'MMM yyyy') as month_date,
      to_date(regexp_extract(x, '(\\d{4})$', 1), 'yyyy') as year_date)) dates
FROM ofac_individual
LATERAL VIEW explode(transform(reported_dates_of_birth, x -> lower(trim(x)))) exploded_reported_dates_of_birth as dob),
date_ranges AS (SELECT
   id,
   dob,
   circa,
   transform(dates, x ->
      CASE
        WHEN x.day_date IS NOT NULL THEN
         named_struct("min", date_sub(x.day_date, if(circa, 15, 0)),
                      "max", date_add(x.day_date, if(circa, 15, 0)))
        WHEN x.month_date IS NOT NULL THEN
         named_struct("min", add_months(x.month_date, if(circa, -2, 0)),
                      "max", add_months(last_day(x.month_date), if(circa, 2, 0)))
        WHEN x.year_date IS NOT NULL  THEN
         named_struct("min", add_months(x.year_date, if(circa, -6, 0)),
                      "max", add_months(date_sub(add_months(x.year_date, 12), 1), if(circa, 6, 0)))
        ELSE NULL
      END
   ) date_ranges
FROM date_parts)
SELECT
  id,
  dob,
  circa,
  date_ranges[0].min min_dob,
coalesce(date_ranges[1].max, date_ranges[0].max) max_dob
FROM date_ranges
""")

ofacDobDF.createOrReplaceTempView("ofac_dob")

matchingNamesDF = spark.sql("""SELECT DISTINCT
  o.id ofac_id,
  g.id uk_id,
  o.alias_value ofac_name,
  g.alias_value uk_name,
  o.place_of_birth ofac_place_of_birth,
  g.place_of_birth uk_place_of_birth,
  o.nationality   ofac_nationality,
  g.nationality   uk_nationality
FROM gbr_individual_data g
JOIN ofac_individual_data o ON (levenshtein(g.alias_value,o.alias_value) < 3)
""")
matchingNamesDF.cache() # for incremental debugging
matchingNamesDF.createOrReplaceTempView("name_matches")

# Calculate matches by further restricting name matches by birth date range matching
# Rank the data by matching criteria, along with some additonal "hacky" overlap measures, taking the "best"
# value by ordering clause.  Using row_number (calling it rank), rank/dense rank may result in multiple rows
# within the same window having a rank of 1 (where row_number simply assign a monotonically icreasing value).
individualMatchesDF = spark.sql("""WITH results AS (SELECT
  m.uk_id,
  m.ofac_id,
  levenshtein(m.uk_name, m.ofac_name) name_distance,
  abs(datediff(gb.min_dob, ob.min_dob)) dob_distance,
  size(array_intersect(transform(split(m.uk_place_of_birth, '[,]'), x -> trim(x)),
                       transform(split(m.ofac_place_of_birth, '[,]'), x -> trim(x)))) place_of_birth_overlap, 
  size(array_intersect(transform(split(m.uk_nationality, '[,]'), x -> trim(x)),
                       transform(split(m.ofac_nationality, '[,]'), x -> trim(x)))) nationality_overlap, 
  m.uk_name,
  m.ofac_name,
  gb.dob uk_dob,
  ob.dob ofac_dob,
  gb.min_dob uk_min_dob,
  ob.min_dob ofac_min_dob,
  m.uk_place_of_birth,
  m.ofac_place_of_birth,
  m.uk_nationality,
  m.ofac_nationality
FROM name_matches m
JOIN gbr_dob gb ON (gb.id = m.uk_id)
JOIN ofac_dob ob ON (ob.id = m.ofac_id)
WHERE gb.min_dob between ob.min_dob and ob.max_dob
   OR gb.max_dob between ob.min_dob and ob.max_dob),
rankings as (
SELECT DISTINCT
  uk_id,
  ofac_id,
  uk_name,
  ofac_name,
  uk_dob,
  ofac_dob,
  uk_place_of_birth,
  ofac_place_of_birth,
  uk_nationality,
  ofac_nationality,
  name_distance,
  dob_distance,
  place_of_birth_overlap,
  nationality_overlap,
  ROW_NUMBER() OVER (PARTITION BY /*uk_id,*/ ofac_id ORDER BY name_distance, dob_distance, place_of_birth_overlap desc, nationality_overlap desc, uk_min_dob) rank
FROM results
ORDER BY uk_id, ofac_id, rank)
SELECT *
FROM rankings
WHERE rank = 1
ORDER BY uk_id, ofac_id, uk_name
""")

print("calculating results...")
individualMatchesDF.show(truncate=False)
individualMatchesDF.createOrReplaceTempView("individual_matches")
individualMatchesDF.write.mode("overwrite").option("header", True).option("delimiter", '\t').csv(output_path)
print(f"Individual match count: {individualMatchesDF.count()}")

print("duplicate detection")

ukDuplicatesDF = spark.sql("""SELECT
ofac_id, COUNT(DISTINCT uk_id) dup_count, COLLECT_SET(uk_id) dup_uk_ids
FROM individual_matches
GROUP BY ofac_id
HAVING COUNT(DISTINCT uk_id) > 1
""")
ukDuplicatesDF.show(20, False)

ofacDuplicatesDF = spark.sql("""SELECT
uk_id, COUNT(DISTINCT ofac_id) dup_count, COLLECT_SET(ofac_id) dup_ofac_ids
FROM individual_matches
GROUP BY uk_id
HAVING COUNT(DISTINCT ofac_id) > 1
""")
ofacDuplicatesDF.show(20, False)

spark.sql("show tables").show()
#spark.stop()

print("done")

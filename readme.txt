Sayari Spark (Matching) Problem
===============================

The problem is broken into 2 separate jobs to keep the code/SQL more readable, as well as to facilitate each job list it's own comparison measures in tabular form.
This could have been done with the jsonl output format; however, the tsv format allows quick upload into a spreadsheet.

The 2 jobs entity_matcher.py and individual_matcher.py can be run in either order.
The results are written in hadoop tsv format; however, for repository check-in purposes the files are just moved to the top level directory for sample output see:

entity_matcher_results.tsv
individual_matcher_results.tsv

The jobs can be run either using spark-submit as:

spark-submit --master "local[*]" entity_matcher.py
spark-submit --master "local[*]" individual_matcher.py

NOTES:

1. The only types that were available in both datasets were Entity and Individual; the other types are not matched.
2. Removing logical duplicates from both gbr and ofac source datasets would have eliminated some duplicates. For now I've just check for duplicates.
3. Additional columns could have been used from both datasets to refine matching; however, just a reasonable subset was used here.

eg., Individual Matching Duplication (annotated console output):
+-----+---------+--------------+-------------------------------------------------------------------------------------------+
|uk_id|dup_count|dup_ofac_ids  | Explanation/Fix                                                                           |
+-----+---------+--------------+-------------------------------------------------------------------------------------------+
|6995 |2        |[11744, 7006] | logical duplicates in source ofac data.  Remove source duplicates before comparing.       |
|8425 |2        |[11743, 8264] | logical duplicates in source ofac data.  Remove source duplicates before comparing.       |
|11091|2        |[11311, 11746]| logical duplicates in source ofac data.  Remove source duplicates before comparing.       |
|11207|2        |[12127, 23629]| possible false positive in name matching in gbr data. Refine scoring/comparison algorithm.|
+-----+---------+--------------+-------------------------------------------------------------------------------------------+

spark.sql("select * from individual_matches where uk_id = 6995").show(truncate=False)
spark.sql("select * from ofac where id in (11744, 7006)").show(truncate=False)


individual_matcher tables:
+---------+--------------------+-----------+-------------------------------------------------------+
|namespace|           tableName|isTemporary|Purspose                                               |
+---------+--------------------+-----------+-------------------------------------------------------+
|         |                 gbr|       true| full input dataset for gbr; all entity types          |
|         |             gbr_dob|       true| exploded dob data for gbr                             |
|         |      gbr_individual|       true| full input dataset for Individual types only from gbr |
|         | gbr_individual_data|       true| gbr exploded Individual data                          |
|         |  individual_matches|       true| final results for this job after scoring matches      |
|         |        name_matches|       true| candidate results for this job before scoring         |
|         |                ofac|       true| full input dataset for ofac; all entity types         |
|         |            ofac_dob|       true| exploded dob data for ofac                            |
|         |     ofac_individual|       true| full input dataset for Individual types only from ofac|
|         |ofac_individual_data|       true| ofac exploded Individual data                         |
+---------+--------------------+-----------+-------------------------------------------------------+

entity_matcher tables:
+---------+----------------+-----------+-----------------------------------------------+
|namespace|       tableName|isTemporary|Purpose                                        |
+---------+----------------+-----------+-----------------------------------------------+
|         |  entity_matches|       true| final results for this job after scoring.     |
|         |             gbr|       true| full input dataset for gbr; all entity types  |
|         |      gbr_entity|       true| only entity type data from gbr                |
|         | gbr_entity_data|       true| exploded entity data from gbr                 |
|         |    name_matches|       true| candidate results for this job before scoring |
|         |            ofac|       true| full input dataset from ofac                  |
|         |     ofac_entity|       true| only entity type data from ofac               |
|         |ofac_entity_data|       true| exploded entity data from ofac                |
+---------+----------------+-----------+-----------------------------------------------+

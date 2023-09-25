Sayari Spark (Matching) Problem
===============================

The problem is broken into 2 separate jobs to keep the code/SQL more readable; additionally,
the each comparison is using different score measurement so the columns to describe those score components are different
for each type.  Alternatively, we could have had the score components all store as json, but that would be less readable in a
spreadsheet.

The 2 jobs entity_matcher.py and individula_matcher.py can be run in either order.
The results are written in hadoop tsv format; however, for repository check-in purposes the files are just moved to the top level directory.

The jobs can be run either using spark-submit as:


spark-submit --master "local[*]" entity_matcher.py
spark-submit --master "local[*]" individual_matcher.py

# DBMemPower

===============

# The workload.sql file contains simple sql queries. Currently, the program uses these queries. Although the queries are very simple and direct most of the queries returns empty result.
# The file tpchoriginalquries.sql file contains the original queries which are generated from tpch-h qgen.exe. However, these queries doesn't execute directly on sqlite3. Hence, I midified the queries, particularly the "date" format and save on separete file named modifiedsqlqueires.sql.
# The modifiedsqlqueires.sql file contains the modified version of the original sql queries, but it doesn't contain all queries. It only contains queries which doesn't trigger an error though the majorities of the queries return empty results.
# The sqlqueriesError.sql file contains queries whcih triggers an error. It keeps triggering an error though I have done a modification in these quries. Hence,I saved it to separate file. 

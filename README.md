# csv-diff

csv-diff is a tool to make a diff from CSV files.

It uses spark internally so it takes time to launch it.

`java -jar target/csv-diff-0.0.1-SNAPSHOT.jar --created-path cp.csv --deleted-path dp.csv -pk id --new-path new.csv --old-path old.csv --updated-path up.csv`

# StackExchangeDataDumpAnalyzerSingle

## Build
```shell script
mvn clean package 
```

## Running
### Local
#### Setup
##### Spark Version
Use any Spark version compatible with Scala version used in the project.

##### Spark History Server
```shell script
# Create a dir for logs, default file:/tmp/spark-events
mkdir /tmp/spark-events

# start Spark History Server
bash $SPARK_HOME/sbin/start-history-server.sh
```

#### Execution
```shell script
bash run.sh <main class> \
  <master> \ 
  <start date> \
  <end date> \
  <aggregation interval> \
  <badges input file> \
  <comments input file> \
  <post history input file> \
  <post links input file> \
  <posts input file> \
  <tags input file> \
  <users input file> \
  <votes input file> \
  <output file location>
e.g.
bash run.sh pl.epsilondeltalimit.analyzer.StackExchangeDataDumpAnalyzerSingle \
  local[2] \
  2010-01-01 \
  2021-01-01 \
  '8 weeks' \
  /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Badges.xml \
  /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Comments.xml \
  /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/PostHistory.xml \
  /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/PostLinks.xml \
  /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Posts.xml \
  /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Tags.xml \
  /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Users.xml \
  /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Votes.xml \
  output/8weeks
```

### GCP Dataproc example
#### Setup
TODO

#### Execution
```shell script
gcloud dataproc jobs submit spark \
    --cluster=cluster-5cff \
    --class=pl.epsilondeltalimit.analyzer.StackExchangeDataDumpAnalyzerSingle \
    --jars=gs://stack-exchange-data-dump-analyzer-single/StackExchangeDataDumpAnalyzerSingle-0.1-SNAPSHOT-jar-with-dependencies.jar \
    --region=europe-west3 \
    --driver-log-levels root=DEBUG \
    -- 2010-01-01 2021-01-01 '13 weeks' gs://stack-exchange-data-dump/scifi.stackexchange.com/Badges.xml gs://stack-exchange-data-dump/scifi.stackexchange.com/Comments.xml gs://stack-exchange-data-dump/scifi.stackexchange.com/PostHistory.xml gs://stack-exchange-data-dump/scifi.stackexchange.com/PostLinks.xml gs://stack-exchange-data-dump/scifi.stackexchange.com/Posts.xml gs://stack-exchange-data-dump/scifi.stackexchange.com/Tags.xml gs://stack-exchange-data-dump/scifi.stackexchange.com/Users.xml gs://stack-exchange-data-dump/scifi.stackexchange.com/Votes.xml gs://stack-exchange-data-dump-analyzer-single/output/scifi.stackexchange.com/13weeks
```

## Create plots
```shell script
# relative popularity
bash scripts/relative_popularity_plot.sh \
  <result file> \
  <tag name> \
  <aggregation interval> \
  <y axis max> \
  <y axis tics>
e.g.
bash scripts/relative_popularity_plot.sh \
  output/tag\=print-quality/part-00000-9b6e8399-3e48-4a97-a355-4b239b975515.c000.csv \
  print-quality \
  8weeks \
  1.0 \
  0.1

# entries count
bash scripts/entries_count_plot.sh \
  <result file> \
  <tag name> \
  <aggregation interval> \
  <y axis max>
e.g.
bash scripts/entries_count_plot.sh \
  output/tag\=print-quality/part-00000-9b6e8399-3e48-4a97-a355-4b239b975515.c000.csv \
  print-quality \
  8weeks \
  100000
```
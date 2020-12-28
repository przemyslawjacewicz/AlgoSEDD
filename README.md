# StackExchangeDataDumpAnalyzerSingle

## Build
```shell script
mvn clean package
```

## Setup
### Spark
#### Spark Version
Use any Spark version compatible with Scala version used in the project.

#### Spark History Server
```shell script
# Create a dir for logs, default file:/tmp/spark-events
mkdir /tmp/spark-events

# start Spark History Server
bash $SPARK_HOME/sbin/start-history-server.sh
```

## Running
### Manual

```shell script
 bash run.sh local[2] pl.epsilondeltalimit.analyzer.StackExchangeDataDumpAnalyzerSingle "8 weeks" /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Badges.xml /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Comments.xml /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/PostHistory.xml /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/PostLinks.xml /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Posts.xml /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Tags.xml /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Users.xml /mnt/datastore/data/StackExchangeDataDump/2020-12-08/3dprinting.stackexchange.com/Votes.xml output
```
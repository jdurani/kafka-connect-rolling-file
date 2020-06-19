# Description
This is a simple kafka connector which stores/reads data to/from rolling files.
The primary goal of this connector is to provide simple and manageable way
to backup and restore data from Kafka topics on local file system without need of
managing external datastores like S3 or HDFS.

# Configuration
## Sink job
|Property|Importance|Type|Default value|Description|
|---|---|---|---|---|
|`rolling.file.directory`|HIGH|String| |Directory to write data to.|
|`rolling.file.flush.count`|MEDIUM|LONG|100000|Number of records after which next file will be rolled.|
|`rolling.file.flush.ms`|MEDIUM|LONG|10000|Time in milliseconds after which next file will be rolled.  This is rather fixed rate at which to roll file than time between two consequent rolls. I.e. roll of a file may happen before flush time elapses since last roll which was triggered by `rolling.file.flush.count`|

_*Example configuration:*_ 
```properties
connector.class=org.jdurani.rollingfile.sink.RollingFileSinkConnector
name=sink-rolling-file
topics=topic1
tasks.max=1
# currently only suported values are
# org.apache.kafka.connect.converters.ByteArrayConverter
# and org.apache.kafka.connect.storage.StringConverter
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
rolling.file.directory=/data/backup
rolling.file.flush.count=100000
rolling.file.flush.ms=10000
```

## Source job
|Property|Importance|Type|Default value|Description|
|---|---|---|---|---|
|`rolling.file.directory`|HIGH|String| |Directory to load data from.|
|`rolling.file.batch.size`|MEDIUM|INT|10000|Number of records to read and send to Kafka in one batch.|
|`rolling.file.ignore.timestamp`|MEDIUM|BOOLEAN|false|Ignore stored timestamps of messages? If ignored, producer will assign timestamp based on current time.|
|`rolling.file.ignore.partition`|MEDIUM|BOOLEAN|false|Ignored stored partitions of messages? If ignored, producer will assign partition based on key and partitioner.|

_*Example configuration:*_ 
```properties
connector.class=org.jdurani.rollingfile.source.RollingFileSourceConnector
name=source-rolling-file
# source task ignores topic because topic name is determined based on stored data
topics=topic-is-ignored
tasks.max=1
# currently only suported value is org.apache.kafka.connect.converters.ByteArrayConverter
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
rolling.file.directory=/data/backup
rolling.file.batch.size=10000
```

# Build
To build plugin run `mvn clean package` command. After successfull build, install
JAR file as new Kafka Connect plugin. E.g.:

```shell script
# path to your kafka connect plugins directory
PATH_TO_KAFKA_CONNECT_PLUGINS=...
RF_PLUGIN_PATH="${PATH_TO_KAFKA_CONNECT_PLUGINS}/kafka-connect-rolling-file/"
mkdir -p ${RF_PLUGIN_PATH}
cp target/rolling-file-kafka-connect-*.jar ${RF_PLUGIN_PATH}
```

## Docker
If you use docker, then simply run `build.sh` scritp. It will build plugin and docker image with installed plugin.
To deploy image to docker swarm, you may may take a look at a `docker-compose.yml` file.

# KafkaOffsetFinder
A small tool for finding partition offsets in an Apache Kafka topic.
We use it to start replays in Apache Flink jobs at a given point in time.
The current level of quality is "works for me"

## Building
sbt clean compile package

Then include the built jar in your own application.
You will need org.apache.kafka.kafka-clients and org.slf4j.slf4j-api on the classpath.
On the other hand it might be easier to just include the source into your project.

## Usage
    // determine offsets
    val offsetFinder = new OffsetFinder[String]
    val offsets = offsetFinder.getOffsets(
      kafkaSourceTopic,
      kafkaProps,
      s => {
        // put some actual code here that
        // can compute a timestamp from one of your Kafka messages
        0L
        },
      // this is the timestamp to look for  
      1501601790720L  
      )
    
    // pass offsets to Flink´s Kafka consumer
    val kafkaOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long] 
    for (o <- offsets) {
      kafkaOffsets.put(new KafkaTopicPartition(kafkaSourceTopic, o._1.partition()), o._2)
    }    
    val consumer = new FlinkKafkaConsumer010[Tuple](kafkaSourceTopic, ...)
    consumer.setStartFromSpecificOffsets(kafkaOffsets)

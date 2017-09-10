package de.noris.offsetfinder

import java.util
import java.util.Properties

import collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import scala.language.postfixOps

import scala.util.Random


/**
  * This class serves to find offsets in Kafka that match a given timestamp as closely as possible.
  * The logic for calculating a timestamp from a Kafka message is passed in as a lambda.
  *
  * Offsets are determined by doing a binary search on all available data for each partition.
  * Rough complexity estimation (NUDE-specific):
  * - our topics are configured to keep up to 80 Gigs of data in 20 partitions
  * - this means 4 Gigs per partition and thus about 100.000.000 records/partition tops
  * - we need up to log2(100.000.000) ~ 27 seeks per partition, 540 total. Each requires at least one roundtrip to Kafka
  * - this should still be done in a couple of seconds
  *
  * The binary search relies on data being ordered per partition. If you have skew within a partition, just adjust
  * the seektime by your expected out-of-orderness.
  *
  * @tparam T The value type of the Kafka messages
  */
class KafkaOffsetFinder[T] {
  val log = LoggerFactory.getLogger(this.getClass)

  val POLL_TIMEOUT = 200

  /**
    * This method returns the offset for each partition where the record matches the given timestamp as well as possible.
    *
    * @param topic      The name of the Kafka topic to connect to
    * @param kafkaProps Properties for connecting to Kafka
    *                   Using the same properties as for your job should work.
    *                   Some properties will be overridden (but your copy is unchanged).
    * @param extract    Function that extracts a timestamp from a message
    * @param seekTime   The point in (application) time that you want offsets for

    * @return           A List of partitions with their suggested offsets
    */
  def getOffsets(topic: String, kafkaProps: Properties, extract: T => Long, seekTime: Long): List[(TopicPartition, Long)] = {
    val consumer = getConsumer(kafkaProps)
    consumer.subscribe(util.Arrays.asList(topic))
    consumer.poll(POLL_TIMEOUT) // force assignment of partitions
    val partitions  = consumer.assignment().asScala.toList
    val result = for (partition <- partitions)
      yield (partition, checkOnePartition (partition, consumer, extract, seekTime, partitions))
    consumer.close()
    result
  }

  /**
    * This method returns a suggested offset for each partition.
    * The expected timestamp is not passed as a parameter here but is computed from the earliest available data.
    * Consider you have two partitions dating back to Monday and Wednesday respectively.
    * This method will return offsets for all partitions starting on Wednesday.
    * So you can process (without skew) the data you still have in all your partitions.
    *
    * @param topic      The name of the Kafka topic to connect to
    * @param kafkaProps Properties for connecting to Kafka
    *                   Using the same properties as for your job should work.
    *                   Some properties will be overridden (but your copy is unchanged).
    * @param extract    Function that extracts a timestamp from a message

    * @return           A List of partitions with their suggested offsets
    */
  def getOffsets(topic: String, kafkaProps: Properties, extract: T => Long): List[(TopicPartition, Long)] = {
    var startTs = Long.MinValue
    val consumer = getConsumer(kafkaProps)
    consumer.subscribe(util.Arrays.asList(topic))
    consumer.poll(POLL_TIMEOUT) // force assignment of partitions
    val partitions = consumer.assignment().asScala.toList
    for (partition <- partitions) {
      getFirstRecord(partition, consumer, partitions) match {
        case Some(record) => startTs = Math.max(startTs, extract(record.value()))
        case _ => // do nothing
      }
    }
    consumer.close()
    getOffsets(topic, kafkaProps, extract, startTs)
  }

  /**
    * Helper to create a KafkaConsumer.
    * It is the duty of the caller to close the consumer after using it.
    *
    * @param kafkaProps Properties
    * @return           KafkaConsumer
    */
  protected def getConsumer(kafkaProps: Properties): KafkaConsumer[String, T] = {
    val props = kafkaProps.clone().asInstanceOf[Properties]
    props.put("enable.auto.commit", "false")
    props.put("group.id", Random.alphanumeric take 16 mkString)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    new KafkaConsumer[String, T](props);
  }

  /**
    * Read a Kafka record from a given offset in a given partition
    *
    * @param partition Number of the partition
    * @param offset    Offset to read from
    * @param consumer  Configured KafkaConsumer
    * @param partitions List of all partitions in the topic. Required for disabling reading from the unwanted partitions*
    * @return          The record read from the given offset or None if no such record could be found
    */
  protected def get(partition: TopicPartition, offset: Long, consumer: KafkaConsumer[String, T], partitions: List[TopicPartition]): Option[ConsumerRecord[String, T]] = {
    consumer.pause(partitions.asJava)
    consumer.resume(List(partition).asJava)
    val records = consumer.poll(POLL_TIMEOUT)
    val iter = records.iterator()
    while (iter.hasNext()) {
      val record = iter.next()
      if (record.partition() == partition.partition()) {
        return Some(record)
      }
    }
    None
  }

  /**
    * Returns the first record in a partition (or None if that cannot be read)
    *
    * @param partition  Partition to read from
    * @param consumer   Configured KafkaConsumer
    * @param partitions List of all partitions in the topic. Required for disabling reading from the unwanted partitions
    * @return           The first record in the partition (or None if that cannot be read)
    */
  protected def getFirstRecord(partition: TopicPartition, consumer: KafkaConsumer[String, T], partitions: List[TopicPartition]): Option[ConsumerRecord[String, T]] = {
    consumer.seekToBeginning(List(partition).asJava)
    val first = consumer.position(partition)
    get(partition, first, consumer, partitions)
  }

  /**
    * Returns the offset for one partition where the message´s timestamp best matches the given timestamp.
    *
    * @param partition  The partition to read from
    * @param consumer   Configured KafkaConsumer
    * @param extract    Function that extracts a timestamp from a message
    * @param seekTime   The point in (application) time that you want offsets for
    * @param partitions List of all partitions in the topic. Required for disabling reading from the unwanted partitions
    * @return           Optimal offset for this partition. If anything goes wrong, the first available offset in the partition is returned.
    */
  protected def checkOnePartition(partition: TopicPartition, consumer: KafkaConsumer[String, T], extract: T => Long, seekTime: Long, partitions: List[TopicPartition] ): Long = {
    consumer.seekToBeginning(List(partition).asJava)
    val first = consumer.position(partition)
    consumer.seekToEnd(List(partition).asJava)
    val last = consumer.position(partition) -1 // -1 because seekToEnd seeks after the last element

    log.trace(s"Partition: ${partition.partition()}")
    log.trace(s"First offset: $first")
    log.trace(s"Last offset: $last")
    log.trace(s"Looking for $seekTime")

    consumer.pause(partitions.asJava)
    consumer.resume(List(partition).asJava)

    val delta = last-first
    var step: Long = delta/2
    var current: Long = first+step
    var direction = 1
    while (step > 1) {
      consumer.seek(partition, current)
      val recordOption = get(partition, current, consumer, partitions)
      recordOption match {
        case Some(record) => {
          val timestamp = extract (record.value () )
          if (timestamp > seekTime) {
            log.trace(s"timestamp at position $current  is $timestamp (too big)")
            direction = - 1
          } else if (timestamp < seekTime) {
            log.trace(s"timestamp at position $current  is $timestamp (too small)")
            direction = 1
          } else {
            log.trace(s"timestamp at position $current  is $timestamp (match)")
            return current
          }
        }
        case None => {
          log.warn(s"Could not get record at position $current from partition ${partition.partition}")
          return first
        }

      }
      step = step / 2
      current = current + step*direction
    }
    return current
  }

}
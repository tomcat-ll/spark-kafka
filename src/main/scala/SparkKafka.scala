
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkStreamKafka1")
   /* //解决部署到服务器，缺少hdfs
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")*/
    //解决部署到服务器，内存不足
    conf.set("spark.testing.memory", "2147480000")
    val sc=new SparkContext(conf)
    //5秒处理一次
    val ssc=new StreamingContext(sc, Seconds(5))
    //实现累加，设置检查点
    ssc.checkpoint("hdfs://hadoop01:9000/chk")

    //zookeeper集群
    val bootstrapServers = "hadoop01:9092"
    //组
    val groupId = "kafka-test-group"
    //主题名t
    val topicName = "test"
    val maxPoll = 500
//kafka参数--map格式
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    //创建流
    //参数配置重难点
    val kafkaSource= KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))

    val lines = kafkaSource.map(_.value())
    val words = lines.flatMap(_.split(" "))
    //叠加
    val wordCounts = words.map(x => (x, 1)).updateStateByKey{(seq, op:Option[Int]) => { Some(seq.sum +op.getOrElse(0)) }}

    wordCounts.repartition(1).saveAsTextFiles("hdfs://hadoop01:9000/word/wordcount08")
    wordCounts.print()


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}

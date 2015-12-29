package com.spnotes.kafka
import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}

object Producer{
  def main(argv:Array[String]): Unit ={
    println("Entering Producer.main")
    if(argv.length != 1){
      println("Please provide name of the topic")
      System.exit(1)
    }

    val topicName = argv(0)
    val producerConfig = new Properties()
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer")
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[Array[Byte], String](producerConfig)

    for(msg <- io.Source.stdin.getLines()){
      if(msg.equals("exit")){

      }
      kafkaProducer.send( new ProducerRecord[Array[Byte],String]("test",null,msg))

    }

    kafkaProducer.close()

    println("Exiting Producer.main")

  }
}
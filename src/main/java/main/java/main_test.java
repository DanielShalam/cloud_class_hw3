package main.java;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class main_test {
  public static void main(String[] args){
    //Configuring the stream
    Properties prop = new Properties();

    prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe"); //Giving the stream an ID
    prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //Telling the stream where the brokers are
    prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // Building and defining the Stream
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("streams-plaintext-input").to("streams-pipe-output"); //The stream each string is a topic
    //Get info from topic one and send it to the other topic

    //Topology item
    final Topology topology = builder.build();

    //Kafka Stream object
    final KafkaStreams streams = new KafkaStreams(topology,prop);
    final CountDownLatch latch = new CountDownLatch(1); //No idea was in the code I worked with

    // Enabling the option to press ctrl+c to close
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
      @Override
      public void run(){
        streams.close();
        latch.countDown();
      }
    });


    try{
      //Starting the stream
      streams.start();
      latch.await();
    } catch (Throwable e){
      System.exit(1);
    }
    System.exit(0);
  }
}

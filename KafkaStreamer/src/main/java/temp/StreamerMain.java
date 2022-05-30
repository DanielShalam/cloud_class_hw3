package temp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// LocalDateTime formatDateTime = LocalDateTime.parse(now, formatter);

public class StreamerMain {
    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public static void main(final String[] args) throws Exception {
        String input_topic = "dtest";
        String output_topic = "outputTopic";
        String time_topic = "timeTopic";
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Bobtest1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // out custom wikiSerde
        JsonSerializer<CreatedEvent> wikiJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<CreatedEvent> wikiJsonDeserializer = new JsonDeserializer<>(CreatedEvent.class);
        Serde<CreatedEvent> wikiSerde = Serdes.serdeFrom(wikiJsonSerializer,wikiJsonDeserializer);

        // build stream worker
        StreamsBuilder builder = new StreamsBuilder();
        try {
           KStream<String, CreatedEvent> initialTable = builder.stream(input_topic, Consumed.with(Serdes.String(), wikiSerde));

           filterByLanguage(initialTable);
//           combineAll(initialTable);

           // Total count of page created events
//           initialTable.
//                    map((key, value) -> new KeyValue<>("Dummy", value.getPage_id())).
//                    groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().
//                    toStream().mapValues(v -> v.toString() + " Pages added").
//                    to(output_topic, Produced.with(Serdes.String(), Serdes.String()));


//            LocalDateTime currentTime = LocalDateTime.now();
//           // Count last month created events
//           initialTable.filter((key, value) -> currentTime.getYear() == LocalDateTime.parse(value.getTimestamp(), formatter).getYear() && currentTime.getMonthValue() == LocalDateTime.parse(value.getTimestamp(), formatter).getMonthValue()).
//                   map((key, value) -> new KeyValue<>("Dummy", value.getPage_id())).
//                   groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().
//                   toStream().mapValues(v -> v.toString() + " Pages added on month: " + currentTime.getMonth() ).
//                   to(time_topic, Produced.with(Serdes.String(), Serdes.String()));

//            // Count last week created events
//            initialTable.filter((key, value) -> (currentTime.getYear() == LocalDateTime.parse(value.getTimestamp(), formatter).getYear() && currentTime.getDayOfYear() - LocalDateTime.parse(value.getTimestamp(), formatter).getDayOfYear() <= 7)).
//                    map((key, value) -> new KeyValue<>("Dummy", value.getPage_id())).
//                    groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().
//                    toStream().mapValues(v -> v.toString() + " Pages added in the last week").
//                    to(time_topic, Produced.with(Serdes.String(), Serdes.String()));
//
//            // Count same day created events
//            initialTable.filter((key, value) -> (currentTime.getYear() == LocalDateTime.parse(value.getTimestamp(), formatter).getYear() && currentTime.getDayOfYear() == LocalDateTime.parse(value.getTimestamp(), formatter).getDayOfYear())).
//                    map((key, value) -> new KeyValue<>("Dummy", value.getPage_id())).
//                    groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().
//                    toStream().mapValues(v -> v.toString() + " Pages added in the last day").
//                    to(time_topic, Produced.with(Serdes.String(), Serdes.String()));

            // Count last hour created events

        } catch (Exception s) {
            s.printStackTrace();
        }

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.setUncaughtExceptionHandler((Thread t, Throwable e) -> System.out.println(e));
        streams.start();
    }


    public static void filterByLanguage(KStream<String, CreatedEvent> stream) {
        // Count last week created events
        stream.map((key, value) -> new KeyValue<>(value.getDomain().split("\\.")[0], value.getPage_id())).
                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().
                toStream().map((key, value) -> new KeyValue<> (key, "Language: " + key + " " + value.toString() + " Pages added")).
                to("timeTopic", Produced.with(Serdes.String(), Serdes.String()));;
    }

    public static void combineAll(KStream<String, CreatedEvent> stream) {
        // Count last week created events
        stream.map((key, value) -> new KeyValue<>(value.getDomain().split("\\.")[0], value.getPage_id())).
                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().
                toStream().map((key, value) -> new KeyValue<> (key, "Language: " + key + " " + value.toString() + " Pages added")).
                to("timeTopic", Produced.with(Serdes.String(), Serdes.String()));;
    }


}
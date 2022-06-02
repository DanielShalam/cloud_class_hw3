package temp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.*;

public class StreamerMain {
    static String input_created_topic = "create-topic";
    static String input_modified_topic = "edit-topic";

    public static void main(final String[] args) throws Exception {
        String output_topic = "outputTopic";
        String time_topic = "timeTopic";

        // initialize properties for out stream workers
        Properties properties = StreamerMain.getProperties("wiki-streamer");

        // out custom wikiSerde
        JsonSerializer<CreatedEvent> wikiJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<CreatedEvent> wikiJsonDeserializer = new JsonDeserializer<>(CreatedEvent.class);
        Serde<CreatedEvent> wikiSerde = Serdes.serdeFrom(wikiJsonSerializer,wikiJsonDeserializer);

        // out custom TopKSerde
        JsonSerializer<GenericTopK> topKJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<GenericTopK> topKJsonDeserializer = new JsonDeserializer<>(GenericTopK.class);
        Serde<GenericTopK> topKSerde = Serdes.serdeFrom(topKJsonSerializer,topKJsonDeserializer);

        // build stream worker
        StreamsBuilder builder = new StreamsBuilder();

        String createMethod = "language+week";
        String modifyMethod = "month+language";

        try {
            KStream<String, CreatedEvent> create_stream = builder.
                    stream(input_created_topic, Consumed.with(Serdes.String(), wikiSerde));

            KStream<String, CreatedEvent> modify_stream = builder.
                    stream(input_modified_topic, Consumed.with(Serdes.String(), wikiSerde));

            mostActiveUsers(create_stream, modify_stream, topKSerde, output_topic);
//            create_stream = processStream(create_stream, output_topic, createMethod, "Create worker", "added");
//            modify_stream = processStream(modify_stream, output_topic, modifyMethod, "Modify worker", "edited");
//            mostActivePages(modify_stream, output_topic, modifyMethod, topKSerde, 5);

        } catch (Exception s) {
            s.printStackTrace();
        }

        KafkaStreams createStreams = new KafkaStreams(builder.build(), properties);

        createStreams.cleanUp();
        createStreams.setUncaughtExceptionHandler((Thread t, Throwable e) -> System.out.println(e));
        createStreams.start();

//        createStreams.close();
    }

    public static KStream<String, CreatedEvent> processStream(KStream<String, CreatedEvent> stream, String topic,
                                                              String method, String workerName, String type) {
//        // filter by time
//        stream = TimeStreamer.timeFiltering(stream, method);
//        String timeString = TimeStreamer.getTimeString(method);

        String timeString = method;

        // group by certain property, language or none for now
        if (method.contains("language")) {
            streamByLanguage(stream, topic, timeString, workerName, type);

        } else if (method.contains("user")) {
            streamByUserType(stream, topic, timeString, workerName, type);

        } else {
            streamAll(stream, topic, timeString, workerName, type);
        }
        return stream;
    }

    public static void mostActiveUsers(KStream<String, CreatedEvent> createdStream,
                                       KStream<String, CreatedEvent> modifyStream,
                                       Serde<GenericTopK> topKSerde, String output_topic) {

        KTable<String, Long> createdKTable = createdStream.
                map((key, value) -> KeyValue.pair(value.getUser_name(), "")).
                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).
                count();

        KTable<String, Long> modifyKTable = modifyStream.
                map((key, value) -> KeyValue.pair(value.getUser_name(), "")).
                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).
                count();

        // outer join
        KTable<String, Long> joinKTable = createdKTable.outerJoin(modifyKTable,
                (leftValue, rightValue) -> {
                if (leftValue == null){
                    leftValue = 0L;
                }
                if (rightValue == null){
                    rightValue = 0L;
                }
                return leftValue + rightValue;
        } /* ValueJoiner */
                /* ValueJoiner */);

        KTable<String, GenericTopK> topKKTable = topK(joinKTable, " ", " ", topKSerde, 5);

        // now we need to publish the results:
        topKKTable.toStream().
                mapValues(value -> "Most active Users (5):\n" + value.toString()).
                to(output_topic, Produced.with(Serdes.String(), Serdes.String()));

    }

    public static KTable<String, GenericTopK> topK(KTable<String, Long> stream, String topic, String method,
                                                   Serde<GenericTopK> topKSerde, int k) {
//        // filter by time
//        stream = TimeStreamer.timeFiltering(stream, method);
//        String timeString = TimeStreamer.getTimeString(method);
        GenericTopK genericClass = new GenericTopK(5);
        // Group all instances by their user type (bot or not) and count for each type
        KTable<String, GenericTopK> topKKTable = stream.
                // 1. set the key as the id and the value to the time_stamp (as string)
//                map((key, value) -> new KeyValue<>(value.getPage_id(), "")).
//                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).
////                count(Materialized.as("count-store")).
//                count().
                // merge key and value for later use
                mapValues((key, value) -> key + GenericTopK.separator + value.toString()).toStream().
                // group all instances by the same key
                groupBy((key, value) -> "Dummy").
                aggregate(() -> genericClass,
                        // the aggregator
                        (key, value, topK) -> {
                            // add the new count to the array
                            genericClass.elementsArray[k] = value;
                            // sort the array by counts, the largest first
                            Arrays.sort(
                                    genericClass.elementsArray, (a, b) -> {
                                        System.out.println(Arrays.toString(topK.elementsArray));
//                                        System.out.println(a);
//                                        System.out.println(b);
                                        // in the initial cycles, some values will be null
                                        if (a == null)  return 1;
                                        if (b == null)  return -1;

                                        String[] parts_a = a.split(GenericTopK.separator);
                                        String[] parts_b = b.split(GenericTopK.separator);

                                        // with two proper CountryMessage objects, do the normal comparison
                                        return Integer.compare(Integer.parseInt(parts_b[1]), Integer.parseInt(parts_a[1]));
                                    }
                            );
                            // set GenericCountHolder k+1 to null
                            genericClass.elementsArray[k] = null;
                            return (genericClass);
                        },

                        Materialized.with(Serdes.String(), topKSerde));

          return topKKTable;
//        // now we need to publish the results:
//        topKKTable.toStream().mapValues(value -> "Most active Pages (5):\n" + value.toString()).
//                to(topic, Produced.with(Serdes.String(), Serdes.String()));

    }

    public static void streamByUserType(KStream<String, CreatedEvent> stream, String topic,
                                        String timeString, String workerName, String type) {
        // Group all instances by their user type (bot or not) and count for each type
        stream.
                map((key, value) -> new KeyValue<>(value.getUser_is_bot(), value.getPage_id())).
                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().
                toStream().map((key, value) -> new KeyValue<> (key, "Worker: " + workerName + ", Is bot: " +
                        key + ", " + value.toString() + " Pages " + type + " " + timeString)).
                to(topic, Produced.with(Serdes.String(), Serdes.String()));;
    }

    public static void streamByLanguage(KStream<String, CreatedEvent> stream, String topic,
                                        String timeString, String workerName, String type) {
        // 1. Filter domains which starts with "www" or "common".
        // 2. Group all instances by their language and count number of instances in each key.
//        TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>();
//        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>();
//        Serde<Windowed<String>> windowsSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        stream.
                filter((key, value) -> !value.getDomain().startsWith("www") && !value.getDomain().startsWith("common")).
                map((key, value) -> new KeyValue<>(value.getDomain().split("\\.")[0], value.getPage_id())).
                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).windowedBy(TimeWindows.of(Duration.ofDays(7))).count().
                toStream().map((key, value) -> new KeyValue <>
                        (key.toString(), "Worker: " + workerName + ", Language: " +
                        key + ", " + value.toString() + " Pages " + type + " " + timeString)).
                to(topic, Produced.with(Serdes.String(), Serdes.String()));;
    }

    public static void streamAll(KStream<String, CreatedEvent> stream, String topic,
                                 String timeString, String workerName, String type) {
//         Total count of page created events
        stream.
                map((key, value) -> new KeyValue<>("Dummy", value.getPage_id())).
                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count().
                toStream().mapValues(v -> v.toString() + " " + type + " added " + timeString + ", Worker: " + workerName).
                to(topic, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static Properties getProperties(String appName) {
        // init streaming properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }


    // define class that will the topK values
//    public class GenericTopK {
//        static String separator = "/S/";
//        static int k = 5;
//        public String[] elementsArray;
//        public GenericTopK() {}
//        public GenericTopK(int k) {
////            this.elementsArray = new GenericCountHolder[k+1];
//            this.elementsArray = new String[k+1];
//            System.out.println("Array: ");
//            System.out.println(Arrays.toString(this.elementsArray));
//        }
//
//        @Override
//        public String toString(){
//            StringBuilder outString = new StringBuilder();
//
//            for (int i = 0; i < this.elementsArray.length - 1; i++) {
//                if (this.elementsArray[i] == null) {
//                    break;
//                }
//                String[] parts = this.elementsArray[i].split(separator);
//                String current = (i+1) + ". ID: " + parts[0] + ", Count: " + parts[1] + "\n";
//                outString.append(current);
//            }
//            return outString.toString();
//        }
//
//        public void initArray(){
//            this.elementsArray = new String[k+1];
//            return;
//        }
//    }


}


//    public static KTable<String, GenericTopK> topK(KTable<String, Long> stream, String topic, String method,
//                                                   Serde<GenericTopK> topKSerde, int k) {
////        // filter by time
////        stream = TimeStreamer.timeFiltering(stream, method);
////        String timeString = TimeStreamer.getTimeString(method);
//
//        // Group all instances by their user type (bot or not) and count for each type
//        KTable<String, GenericTopK> topKKTable = stream.
//                // 1. set the key as the id and the value to the time_stamp (as string)
////                map((key, value) -> new KeyValue<>(value.getPage_id(), "")).
////                groupByKey(Grouped.with(Serdes.String(), Serdes.String())).
//////                count(Materialized.as("count-store")).
////                count().
//                // merge key and value for later use
//                        mapValues((key, value) -> key + GenericTopK.separator + value.toString()).toStream().
//                // group all instances by the same key
//                        groupBy((key, value) -> "Dummy").
//                aggregate(() -> new GenericTopK(k),
//                        // the aggregator
//                        (key, value, topK) -> {
//                            // add the new count to the array
//                            topK.elementsArray[k] = value;
//                            // sort the array by counts, the largest first
//                            Arrays.sort(
//                                    topK.elementsArray, (a, b) -> {
//                                        System.out.println(a);
//                                        System.out.println(b);
//                                        // in the initial cycles, some values will be null
//                                        if (a == null)  return 1;
//                                        if (b == null)  return -1;
//
//                                        String a_val = a.split(GenericTopK.separator)[1];
//                                        String b_val = b.split(GenericTopK.separator)[1];
//                                        // with two proper CountryMessage objects, do the normal comparison
//                                        return Integer.compare(Integer.parseInt(b_val), Integer.parseInt(a_val));
//                                    }
//                            );
//                            // set GenericCountHolder k+1 to null
//                            topK.elementsArray[k] = null;
//                            return (topK);
//                        },
//                        Materialized.with(Serdes.String(), topKSerde));
//
//        return topKKTable;
////        // now we need to publish the results:
////        topKKTable.toStream().mapValues(value -> "Most active Pages (5):\n" + value.toString()).
////                to(topic, Produced.with(Serdes.String(), Serdes.String()));
//
//    }
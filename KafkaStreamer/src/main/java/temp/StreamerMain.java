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

enum timeDuration {
    HOUR,
    DAY,
    WEEK,
    MONTH
}


public class StreamerMain {
    static String input_created_topic = "create-topic";
    static String input_modified_topic = "edit-topic";

    // out custom wikiSerde
    static JsonSerializer<CreatedEvent> wikiJsonSerializer = new JsonSerializer<>();
    static JsonDeserializer<CreatedEvent> wikiJsonDeserializer = new JsonDeserializer<>(CreatedEvent.class);
    public static Serde<CreatedEvent> wikiSerde = Serdes.serdeFrom(wikiJsonSerializer, wikiJsonDeserializer);

    // out custom TopKSerde
    static JsonSerializer<GenericTopK> topKJsonSerializer = new JsonSerializer<>();
    static JsonDeserializer<GenericTopK> topKJsonDeserializer = new JsonDeserializer<>(GenericTopK.class);
    public static Serde<GenericTopK> topKSerde = Serdes.serdeFrom(topKJsonSerializer, topKJsonDeserializer);

    static JsonSerializer<SumClass> SumClassJsonSerializer = new JsonSerializer<>();
    static JsonDeserializer<SumClass> SumClassJsonDeserializer = new JsonDeserializer<>(SumClass.class);
    public static Serde<SumClass> SumClassSerde = Serdes.serdeFrom(SumClassJsonSerializer, SumClassJsonDeserializer);

    public static String outTopic = "outputTopic";

    public static int top_K = 5;

    // 3. to output topic
    public static HashMap<String, String> keyUserMapper = new HashMap<>();

    public static void main(final String[] args) throws Exception {
        // initialize properties for out stream workers
        Properties properties = StreamerMain.getProperties("main-wiki-streamer");

        // Add keys and values (Country, City)
        keyUserMapper.put("true", "Bot");
        keyUserMapper.put("false", "Human");

        // build stream worker
        StreamsBuilder builder = new StreamsBuilder();

        String createdEvent = "Page-created";
        String modifyEvent = "Page-edited";

        try {
            // stream for page added event
            KStream<String, CreatedEvent> createStream = builder.
                    stream(input_created_topic, Consumed.with(Serdes.String(), wikiSerde));

            // stream for modify page event
            KStream<String, CreatedEvent> modifyStream = builder.
                    stream(input_modified_topic, Consumed.with(Serdes.String(), wikiSerde));

            // Event: Page-created
            byTimePipeline(createStream, createdEvent);
            byUserPipeline(createStream, createdEvent);
            byLanguagePipeline(createStream, createdEvent);

            // Event: Page-edited
            byTimePipeline(modifyStream, modifyEvent);
            byUserPipeline(modifyStream, modifyEvent);
            byLanguagePipeline(modifyStream, modifyEvent);

            // Top-K now ....
            topKByUserPipeline(createStream, modifyStream);
            topKByTimePipeline(createStream, modifyStream);
            topKByLanguagePipeline(createStream, modifyStream, "Pages");
            topKByLanguagePipeline(createStream, modifyStream, "Users");

        } catch (Exception s) {
            s.printStackTrace();
        }

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp();
        streams.setUncaughtExceptionHandler((Thread t, Throwable e) -> System.out.println(e));
        streams.start();

//        createStreams.close();
    }

    public static void byTimePipeline(KStream<String, CreatedEvent> stream, String event) throws Exception {
        // Extract events by Time
        // Group events under the same key
        KGroupedStream<String, CreatedEvent> groupedStream = stream.
                map((key, value) -> new KeyValue<>("dummy", value)).groupByKey(Grouped.with(Serdes.String(), wikiSerde));

        // All-time events
        groupedStream.count().toStream().
                map((key, value) -> KeyValue.pair(key, "Event: " + event + ", Duration: All time, Count: " + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        // By time window
        //// Hour
        durationFilter(groupedStream, timeDuration.HOUR).count().
                toStream().map((key, value) -> KeyValue.pair(key.toString(), "Event: " + event + ", Duration: Last Hour, Count: " + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        //// Day
        durationFilter(groupedStream, timeDuration.DAY).count().
                toStream().map((key, value) -> KeyValue.pair(key.toString(), "Event: " + event + ", Duration: Last Day, Count: " + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        //// Week
        durationFilter(groupedStream, timeDuration.WEEK).count().
                toStream().map((key, value) -> KeyValue.pair(key.toString(), "Event: " + event + ", Duration: Last Week, Count: " + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        //// Month
        durationFilter(groupedStream, timeDuration.MONTH).count().
                toStream().map((key, value) -> KeyValue.pair(key.toString(), "Event: " + event + ", Duration: Last Month, Count: " + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void byUserPipeline(KStream<String, CreatedEvent> stream, String event){
        // group instances by User_is_bot and calculate their frequency (count)
        KStream<String, Long> countStream = stream.map((key, value) -> KeyValue.pair(value.getUser_is_bot(), value)).
                                            groupByKey(Grouped.with(Serdes.String(), wikiSerde)).
                                            count().toStream();

        // to output topic
        countStream.map((key, value) -> new KeyValue<>(key, "Event: " + event + ", User type: " + keyUserMapper.get(key)
                        + ", Count: " + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        aggregateCountSum(countStream, event);
    }
    public static void byLanguagePipeline(KStream<String, CreatedEvent> stream, String event) {
        KStream<String, Long> countStream = stream.
                // 1. Filter domains which starts with "www" or "common".
                filter((key, value) -> !value.getDomain().startsWith("www") && !value.getDomain().startsWith("common")).
                // 2. Group all instances by their language and count number of instances in each key.
                map((key, value) -> new KeyValue<>(value.getDomain().split("\\.")[0], value)).
                groupByKey(Grouped.with(Serdes.String(), wikiSerde)).
                // 3. Count
                count().toStream();
                // 4. Send to the output topic

        countStream.map((key, value) -> new KeyValue<>(key, "Event: " + event + ", Language: " +
                key + ", Count: " + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        aggregateCountSum(countStream, event);
    }

    public static void topKByTimePipeline(KStream<String, CreatedEvent> createStream,
                                          KStream<String, CreatedEvent> modifyStream) throws Exception {

        // top-k users in total
        mostActiveUsers(createStream, modifyStream, null,"All Time Most active Users (5): \n", false);

        // By time window
        mostActiveUsers(createStream, modifyStream, timeDuration.HOUR,"Last Hour Most active Users (5): \n", true);
        mostActiveUsers(createStream, modifyStream, timeDuration.DAY,"Last Day Most active Users (5): \n", true);
        mostActiveUsers(createStream, modifyStream, timeDuration.WEEK,"Last Week Most active Users (5): \n", true);
        mostActiveUsers(createStream, modifyStream, timeDuration.MONTH,"Last Month Most active Users (5): \n", true);

        // top-k users in total
        mostActivePages(createStream, modifyStream, null,"All Time Most active Pages (5): \n", false);

        // By time window
        mostActivePages(createStream, modifyStream, timeDuration.HOUR,"Last Hour Most active Pages (5): \n", true);
        mostActivePages(createStream, modifyStream, timeDuration.DAY,"Last Day Most active Pages (5): \n", true);
        mostActivePages(createStream, modifyStream, timeDuration.WEEK,"Last Week Most active Pages (5): \n", true);
        mostActivePages(createStream, modifyStream, timeDuration.MONTH,"Last Month Most active Pages (5): \n", true);
    }

    public static void topKByUserPipeline(KStream<String, CreatedEvent> createStream,
                                          KStream<String, CreatedEvent> modifyStream) throws Exception {

        //// Filter data and get only bot events
        KStream<String, CreatedEvent> botCreatedStream = createStream.
                filter((key, value) -> value.getUser_is_bot().equals("true"));
        KStream<String, CreatedEvent> botModifyStream = modifyStream.
                filter((key, value) -> value.getUser_is_bot().equals("true"));

        // join and find top-k Bots
        mostActiveUsers(botCreatedStream, botModifyStream, null,"Most active Users(Bots) (5): \n", false);
        mostActivePages(botCreatedStream, botModifyStream, null,"Most active Pages(By Bots) (5): \n", false);

        //// Filter data and get only human events
        KStream<String, CreatedEvent> humanCreatedStream = createStream.
                filter((key, value) -> value.getUser_is_bot().equals("false"));
        KStream<String, CreatedEvent> humanModifyStream = modifyStream.
                filter((key, value) -> value.getUser_is_bot().equals("false"));

        // join and find top-k Bots
        mostActiveUsers(humanCreatedStream, humanModifyStream, null, "Most active Users(Humans) (5): \n", false);
        mostActivePages(humanCreatedStream, humanModifyStream, null, "Most active Pages(By Humans) (5): \n", false);
    }

    public static void topKByLanguagePipeline(KStream<String, CreatedEvent> createStream,
                                              KStream<String, CreatedEvent> modifyStream, String eventType) throws Exception {
        KTable<String, Long> createdCountTable;
        KTable<String, Long> modifiedCountTable;

        //// filter and group by domain
        KStream<String, CreatedEvent> createFiltered = createStream.
                // 1. Filter domains which starts with "www" or "common".
                filter((key, value) -> !value.getDomain().startsWith("www") && !value.getDomain().startsWith("common"));

        //// filter and group by domain
        KStream<String, CreatedEvent> modifyFiltered = modifyStream.
                // 1. Filter domains which starts with "www" or "common".
                filter((key, value) -> !value.getDomain().startsWith("www") && !value.getDomain().startsWith("common"));

        if (eventType.startsWith("Page")) {
            createdCountTable = createFiltered.
                    map((key, value) -> KeyValue.pair(value.getDomain().split("\\.")[0] + GenericTopK.separator + value.getPage_id(), value)).
                    groupByKey(Grouped.with(Serdes.String(), wikiSerde)).
                    count();

            modifiedCountTable = createFiltered.
                    map((key, value) -> KeyValue.pair(value.getDomain().split("\\.")[0] + GenericTopK.separator + value.getPage_id(), value)).
                    groupByKey(Grouped.with(Serdes.String(), wikiSerde)).
                    count();
        }
        else {
            createdCountTable = createFiltered.
                    map((key, value) -> KeyValue.pair(value.getDomain().split("\\.")[0] + GenericTopK.separator + value.getUser_name(), value)).
                    groupByKey(Grouped.with(Serdes.String(), wikiSerde)).
                    count();

            modifiedCountTable = createFiltered.
                    map((key, value) -> KeyValue.pair(value.getDomain().split("\\.")[0] + GenericTopK.separator + value.getUser_name(), value)).
                    groupByKey(Grouped.with(Serdes.String(), wikiSerde)).
                    count();
        }

        // outer join for all counts
        KTable<String, Long> joinKTable = createdCountTable.outerJoin(modifiedCountTable,
                (leftValue, rightValue) -> {
                    if (leftValue == null) {
                        leftValue = 0L;
                    }
                    if (rightValue == null) {
                        rightValue = 0L;
                    }
                    return leftValue + rightValue;
                } /* ValueJoiner */
                /* ValueJoiner */);

        // map to key languages
        joinKTable.toStream().map((key, value) -> new KeyValue<>(key.split(GenericTopK.separator)[0],
                key.split(GenericTopK.separator)[1] + GenericTopK.separator + value.toString()));

        // Group all instances by their user type (bot or not) and count for each type
        KTable<String, GenericTopK> topKKTable = joinKTable.toStream().map((key, value) -> new KeyValue<>(key.split(GenericTopK.separator)[0],
                key.split(GenericTopK.separator)[1] + GenericTopK.separator + value.toString())).
                // group all instances by the same key
                groupBy((key, value) -> key).
                aggregate(() -> new GenericTopK(top_K),
                        // the aggregator
                        (key, value, topK) -> {
                            if (topK.isNew(value)){
                                // replace old value with new one
                                return (topK);
                            }
                            // add the new count to the array
                            topK.elementsArray[top_K] = value;
                            // sort the array by counts, the largest first
                            Arrays.sort(
                                    topK.elementsArray, (a, b) -> {
                                        // in the initial cycles, some values will be null
                                        if (a == null) return 1;
                                        if (b == null) return -1;

                                        String[] parts_a = a.split(GenericTopK.separator);
                                        String[] parts_b = b.split(GenericTopK.separator);

                                        // with two proper CountryMessage objects, do the normal comparison
                                        return Integer.compare(Integer.parseInt(parts_b[1]), Integer.parseInt(parts_a[1]));
                                    }
                            );
                            // set GenericCountHolder k+1 to null
                            topK.elementsArray[top_K] = null;
                            return (topK);
                        },

                        Materialized.with(Serdes.String(), topKSerde));

        // now we need to publish the results:
        topKKTable.toStream().
                map((key, value) -> KeyValue.pair(key, "Most active " + eventType + " for Language: " +
                        key + "\n" + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void mostActiveUsers(KStream<String, CreatedEvent> createdStream, KStream<String, CreatedEvent> modifyStream,
                                       timeDuration duration, String eventString, boolean timeWindow) throws Exception {
        KTable<String, Long> createKTable;
        KTable<String, Long> modifyKTable;

        // group stream instances
        KGroupedStream<String, CreatedEvent> createdGroupedTable = createdStream.
                map((key, value) -> KeyValue.pair(value.getUser_name(), value)).
                groupByKey(Grouped.with(Serdes.String(), wikiSerde));

        KGroupedStream<String, CreatedEvent> modifyGroupedTable = modifyStream.
                map((key, value) -> KeyValue.pair(value.getUser_name(), value)).
                groupByKey(Grouped.with(Serdes.String(), wikiSerde));

        if (timeWindow) {
            createKTable = durationFilter(createdGroupedTable, duration).count().
                    toStream().
                    map((key, value) -> KeyValue.pair(key.toString().split("@")[0].substring(1), value)).
                    toTable(Materialized.with(Serdes.String(), Serdes.Long()));

            modifyKTable = durationFilter(modifyGroupedTable, duration).count().
                    toStream().
                    map((key, value) -> KeyValue.pair(key.toString().split("@")[0].substring(1), value)).
                    toTable(Materialized.with(Serdes.String(), Serdes.Long()));
        }
        else {
            createKTable = createdGroupedTable.count();
            modifyKTable = modifyGroupedTable.count();
        }

        // outer join
        KTable<String, Long> joinKTable = createKTable.outerJoin(modifyKTable,
                (leftValue, rightValue) -> {
                    if (leftValue == null) {
                        leftValue = 0L;
                    }
                    if (rightValue == null) {
                        rightValue = 0L;
                    }
                    return leftValue + rightValue;
                } /* ValueJoiner */
                /* ValueJoiner */);
        KTable<String, GenericTopK> topKKTable = topK(joinKTable);

        // now we need to publish the results:
        topKKTable.toStream().
                map((key, value) -> KeyValue.pair(key.toString(), eventString + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void mostActivePages(KStream<String, CreatedEvent> createdStream, KStream<String, CreatedEvent> modifyStream,
                                       timeDuration duration, String eventString, boolean timeWindow) throws Exception {
        KTable<String, Long> createKTable;
        KTable<String, Long> modifyKTable;

        // group stream instances
        KGroupedStream<String, CreatedEvent> createdGroupedTable = createdStream.
                map((key, value) -> KeyValue.pair(value.getPage_id(), value)).
                groupByKey(Grouped.with(Serdes.String(), wikiSerde));

        KGroupedStream<String, CreatedEvent> modifyGroupedTable = modifyStream.
                map((key, value) -> KeyValue.pair(value.getPage_id(), value)).
                groupByKey(Grouped.with(Serdes.String(), wikiSerde));

        if (timeWindow) {
            createKTable = durationFilter(createdGroupedTable, duration).count().
                    toStream().
                    map((key, value) -> KeyValue.pair(key.toString().split("@")[0].substring(1), value)).
                    toTable(Materialized.with(Serdes.String(), Serdes.Long()));

            modifyKTable = durationFilter(modifyGroupedTable, duration).count().
                    toStream().
                    map((key, value) -> KeyValue.pair(key.toString().split("@")[0].substring(1), value)).
                    toTable(Materialized.with(Serdes.String(), Serdes.Long()));
        }
        else {
            createKTable = createdGroupedTable.count();
            modifyKTable = modifyGroupedTable.count();
        }

        // outer join
        KTable<String, Long> joinKTable = createKTable.outerJoin(modifyKTable,
                (leftValue, rightValue) -> {
                    if (leftValue == null) {
                        leftValue = 0L;
                    }
                    if (rightValue == null) {
                        rightValue = 0L;
                    }
                    return leftValue + rightValue;
                } /* ValueJoiner */
                /* ValueJoiner */);
        KTable<String, GenericTopK> topKKTable = topK(joinKTable);

        // now we need to publish the results:
        topKKTable.toStream().
                map((key, value) -> KeyValue.pair(key.toString(), eventString + value.toString())).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));
    }


    public static void aggregateCountSum(KStream<String, Long> stream, String event){
        // function to find the relative part of each value in a key.
        // implemented by aggregating the sum over all counts under the same key.
        KTable<String, SumClass> sumTable = stream.mapValues((key, value) -> (key + GenericTopK.separator + value.toString())).
                groupBy((key, value) -> "1").
                aggregate(() -> new SumClass(),
                        (key, value, aggregate) -> {
                            if (value == null){
                                return aggregate;
                            }
                            aggregate.add(value);
                            return aggregate;
                        },
                        Materialized.with(Serdes.String(), SumClassSerde));

        sumTable.toStream().mapValues((key, value) -> event + " Relative part per key: " + value.toString()).
                to(outTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

    public static KTable<String, GenericTopK> topK (KTable<String, Long> stream) {
        // Group all instances by their user type (bot or not) and count for each type
        KTable<String, GenericTopK> topKKTable = stream.
                // merge key and value for later use
                mapValues((key, value) -> key + GenericTopK.separator + value.toString()).toStream().
                // group all instances by the same key
                groupBy((key, value) -> "Dummy").
                aggregate(() -> new GenericTopK(top_K),
                        // the aggregator
                        (key, value, topK) -> {
                            if (topK.isNew(value)){
                                // replace old value with new one
                                return (topK);
                            }
                            // add the new count to the array
                            topK.elementsArray[top_K] = value;
                            // sort the array by counts, the largest first
                            Arrays.sort(
                                    topK.elementsArray, (a, b) -> {
                                        // in the initial cycles, some values will be null
                                        if (a == null) return 1;
                                        if (b == null) return -1;

                                        String[] parts_a = a.split(GenericTopK.separator);
                                        String[] parts_b = b.split(GenericTopK.separator);

                                        // with two proper CountryMessage objects, do the normal comparison
                                        return Integer.compare(Integer.parseInt(parts_b[1]), Integer.parseInt(parts_a[1]));
                                    }
                            );
                            // set GenericCountHolder k+1 to null
                            topK.elementsArray[top_K] = null;
                            return (topK);
                        },

                        Materialized.with(Serdes.String(), topKSerde));

        return topKKTable;
    }

    public static TimeWindowedKStream<String, CreatedEvent> durationFilter(KGroupedStream<String, CreatedEvent> groupedStream,
                                                                           timeDuration duration) throws Exception {
        switch (duration) {
            case HOUR:
                return groupedStream.windowedBy(TimeWindows.of(Duration.ofHours(1)));
            case DAY:
                return groupedStream.windowedBy(TimeWindows.of(Duration.ofDays(1)));
            case WEEK:
                return groupedStream.windowedBy(TimeWindows.of(Duration.ofDays(7)));
            case MONTH:
                return groupedStream.windowedBy(TimeWindows.of(Duration.ofDays(30)));
        }
        throw new Exception();
    }

    private static Properties getProperties(String appName) {
        // init streaming properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
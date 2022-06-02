package temp;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeStreamer {
    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private static KStream<String, CreatedEvent> yearFilter(KStream<String, CreatedEvent> stream, LocalDateTime currentTime){
        // function to filter same year events (calling by all other methods)
        return stream.filter((key, value) -> currentTime.getYear() ==
                LocalDateTime.parse(value.getTimestamp(), formatter).getYear());
    }

    public static KStream<String, CreatedEvent> filterLastMonth(KStream<String, CreatedEvent> stream) {
        LocalDateTime currentTime = LocalDateTime.now();
        // filter by year
        stream = TimeStreamer.yearFilter(stream, currentTime);
        // keep only last month created events
        stream.filter((key, value) -> currentTime.getMonthValue() == LocalDateTime.parse(value.getTimestamp(), formatter).getMonthValue());
        return stream;
    }

    public static KStream<String, CreatedEvent> filterLastWeek(KStream<String, CreatedEvent> stream) {
        LocalDateTime currentTime = LocalDateTime.now();
        // filter by year
        stream = TimeStreamer.yearFilter(stream, currentTime);
        // keep only last week created events
        stream.filter((key, value) ->  currentTime.getDayOfYear() - LocalDateTime.parse(value.getTimestamp(),
                       formatter).getDayOfYear() <= 7);
        return stream;

    }

    public static KStream<String, CreatedEvent> filterLastDay(KStream<String, CreatedEvent> stream) {
        LocalDateTime currentTime = LocalDateTime.now();
        // filter by year
        stream = TimeStreamer.yearFilter(stream, currentTime);
        // keep only last week created events
        stream.filter((key, value) ->  currentTime.getDayOfYear() == LocalDateTime.parse(value.getTimestamp(),
                       formatter).getDayOfYear());
        return stream;

    }

    //      TODO: Need to be implemented
//    public void filterLastHour(KStream<String, CreatedEvent> stream, String topic) throws Exception {
//    }

    public static KStream<String, CreatedEvent> timeFiltering(KStream<String, CreatedEvent> stream, String streamMethod){
        streamMethod = streamMethod.toLowerCase();
        // choose the time filtering method
        if (streamMethod.contains("month")) {
            // last month filtering
            stream = TimeStreamer.filterLastMonth(stream);

        } else if ((streamMethod.contains("week"))) {
            // last week filtering
            stream = TimeStreamer.filterLastWeek(stream);

        }
        else if ((streamMethod.contains("day"))) {
            // last day filtering
            stream = TimeStreamer.filterLastDay(stream);

        }
        return stream;
    }

    public static String getTimeString(String streamMethod){
        streamMethod = streamMethod.toLowerCase();
        if (streamMethod.contains("month")) {
            return "In the last Month.";
        } else if ((streamMethod.contains("week"))) {
            return "In the last Week.";
        }
        else if ((streamMethod.contains("day"))) {
            return "In the last Day.";
        }
        else {
            return "In total.";
        }
    }

}

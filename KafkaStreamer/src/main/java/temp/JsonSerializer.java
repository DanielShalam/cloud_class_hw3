package temp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

    private ObjectMapper om = new ObjectMapper();

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        // TODO Auto-generated method stub

    }

    @Override
    public byte[] serialize(String topic, T data) {
        byte[] retval = null;
        try {
            retval = om.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            throw new SerializationException();
        }
        return retval;
    }

}
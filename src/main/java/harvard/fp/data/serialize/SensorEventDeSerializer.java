package harvard.fp.data.serialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import harvard.fp.data.model.SensorEvent;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SensorEventDeSerializer implements Deserializer<SensorEvent> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public SensorEvent deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        SensorEvent sensorEvent;
        try {
            sensorEvent = objectMapper.readValue(data, SensorEvent.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return sensorEvent;
    }

    @Override
    public void close() {

    }
}

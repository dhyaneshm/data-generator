package harvard.fp.data.uti;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import harvard.fp.data.model.SensorEvent;

public class SensorEventParser {
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static SensorEvent getSensorEventFromString(String sensorEventAsString) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(sensorEventAsString, SensorEvent.class);
    }

    public static String getSensorEventAsJsonString(SensorEvent sensorEvent) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(sensorEvent);
    }
}

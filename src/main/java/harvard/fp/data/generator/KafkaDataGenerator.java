package harvard.fp.data.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import harvard.fp.data.kafka.MessageProducer;
import harvard.fp.data.model.SensorEvent;
import harvard.fp.data.uti.FileReader;
import harvard.fp.data.uti.SensorEventParser;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Randomly picks sample data from input directory and generates sensor events in json format
 * to output directory
 */
public class KafkaDataGenerator {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final Instant DEFAULT_START_DATE = Instant.now().truncatedTo(ChronoUnit.DAYS);
    public static final String DEFAULT_DAYS = "7";
    public static final String DEFAULT_TEST_TOPIC = "test";
    public static final String DEFAULT_KAFKA_URL = "localhost:9092";
    private String kafkaTopic ;
    private String kafkaUrl ;
    Set<String> sensorIdList = FileReader.readAllValuesFile("input/sensor-id.txt");
    String[] sensorIdArray, sensorTypeArray, windDirectionArray, zipCodeArray;

    Set<String> sensorTypeList = FileReader.readAllValuesFile("input/sensor-type.txt");

    Set<String> windDirectionList = FileReader.readAllValuesFile("input/wind-directions.txt");

    Set<String> zipCodeList = FileReader.readAllValuesFile("input/zipcode.txt");

    Instant dayBeginningEpoch = Instant.now().truncatedTo(ChronoUnit.DAYS);
    MessageProducer kafkaProducer = null ;
    int numberOfDaysRange ;
    long daysInMillis;
    public KafkaDataGenerator() {
        sensorIdArray =  sensorIdList.stream().toArray(String[]::new);
        sensorTypeArray =  sensorTypeList.stream().toArray(String[]::new);
        zipCodeArray =  zipCodeList.stream().toArray(String[]::new);
        windDirectionArray =  windDirectionList.stream().toArray(String[]::new);
        String startDateAsEpochString = System.getProperty("start_date_epoch_millis",null);
        String endDaysFromStartString = System.getProperty("end_no_days_from_start", DEFAULT_DAYS);
        validateStartParameters(startDateAsEpochString, endDaysFromStartString);
        daysInMillis = TimeUnit.DAYS.toMillis(numberOfDaysRange);

        String outputDirectory = System.getProperty("output_dir","src/main/resources/output");
        kafkaTopic = System.getProperty("kafka_topic", DEFAULT_TEST_TOPIC);
        kafkaUrl = System.getProperty("kafka_url", DEFAULT_KAFKA_URL);
        kafkaProducer = new MessageProducer(kafkaUrl, kafkaTopic);
    }

    private void validateStartParameters(String startDateAsEpochString, String endDaysFromStartString) {
        if(startDateAsEpochString == null) {
            dayBeginningEpoch = DEFAULT_START_DATE;
        }else{
            try {
                dayBeginningEpoch = Instant.ofEpochMilli(Long.parseLong(startDateAsEpochString));
            } catch (NumberFormatException e) {
                System.out.println("invalid date. using default start date");
                dayBeginningEpoch = DEFAULT_START_DATE;
            }
        }

        try {
            numberOfDaysRange = Integer.parseInt(endDaysFromStartString);
        } catch (NumberFormatException e) {
            numberOfDaysRange = Integer.parseInt(DEFAULT_DAYS);
        }
    }

    public void generateData() throws IOException {
        for(int i=0;i<500;i++){
            SensorEvent event = new SensorEvent();

            event.setEventId(UUID.randomUUID().toString());
            long randomEventTimestampWithinRange = dayBeginningEpoch.toEpochMilli() + ThreadLocalRandom.current().nextLong(0, daysInMillis);
            event.setEventTimestamp(randomEventTimestampWithinRange);

            int sensorIdRandomIndex = ThreadLocalRandom.current().nextInt(0, sensorIdList.size());
            event.setSensorId(sensorIdArray[sensorIdRandomIndex]);

            int sensorTypeRandomIndex = ThreadLocalRandom.current().nextInt(0, sensorTypeList.size());
            event.setSensorType(sensorTypeArray[sensorTypeRandomIndex]);

            int windDirectionRandomIndex = ThreadLocalRandom.current().nextInt(0, windDirectionList.size());
            event.setWindDirection(windDirectionArray[windDirectionRandomIndex]);

            int zipCodeRandomIndex = ThreadLocalRandom.current().nextInt(0, zipCodeList.size());
            String zipCode = zipCodeArray[zipCodeRandomIndex];
            zipCode = StringUtils.stripStart(zipCode, "\"");
            zipCode = StringUtils.stripEnd(zipCode, "\"");
            event.setZipCode(zipCode);

            Double humidity = ThreadLocalRandom.current().nextDouble(50.0, 90.0);
            event.setHumidityPercentage(humidity.floatValue());

            event.setWindSpeedInMPH(ThreadLocalRandom.current().nextInt(0,85));
            event.setTemperatureInCelcius(ThreadLocalRandom.current().nextInt(-20,50));

            kafkaProducer.sendMessage(kafkaTopic, SensorEventParser.getSensorEventAsJsonString(event));
        }
    }

}

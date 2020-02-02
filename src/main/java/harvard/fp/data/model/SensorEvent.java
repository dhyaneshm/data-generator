package harvard.fp.data.model;

import java.util.Objects;

public class SensorEvent {
    private String eventId;
    private String sensorId;
    private String sensorType;
    private String windDirection;
    private long eventTimestamp;
    private String zipCode;

    private float humidityPercentage;
    private long windSpeedInMPH;
    private float pressureinIN;
    private float temperatureinCelcius;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getSensorType() {
        return sensorType;
    }

    public void setSensorType(String sensorType) {
        this.sensorType = sensorType;
    }

    public String getWindDirection() {
        return windDirection;
    }

    public void setWindDirection(String windDirection) {
        this.windDirection = windDirection;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public float getHumidityPercentage() {
        return humidityPercentage;
    }

    public void setHumidityPercentage(float humidityPercentage) {
        this.humidityPercentage = humidityPercentage;
    }

    public long getWindSpeedInMPH() {
        return windSpeedInMPH;
    }

    public void setWindSpeedInMPH(long windSpeedInMPH) {
        this.windSpeedInMPH = windSpeedInMPH;
    }

    public float getPressureinIN() {
        return pressureinIN;
    }

    public void setPressureinIN(float pressureinIN) {
        this.pressureinIN = pressureinIN;
    }

    public float getTemperatureinCelcius() {
        return temperatureinCelcius;
    }

    public void setTemperatureinCelcius(float temperatureinCelcius) {
        this.temperatureinCelcius = temperatureinCelcius;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorEvent that = (SensorEvent) o;
        return eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }
}

package fi.tkgwf.ruuvi.db;

import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;
import fi.tkgwf.ruuvi.config.Config;
import fi.tkgwf.ruuvi.utils.InfluxDBConverter;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.write.Point;

public class InfluxDBConnection implements DBConnection {

    private final InfluxDBClient influxDBClient;
    private static final String BUCKET = "ruuvi";
    private WriteApi writeApi;

    public InfluxDBConnection() {
        this(Config.getInfluxUrl(), Config.isInfluxGzip(), Config.getInfluxOrg(), Config.getInfluxToken());
    }

    public InfluxDBConnection(String url, boolean gzip, String org, String token) {
        influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org, BUCKET);
        writeApi = influxDBClient.makeWriteApi();
        if (gzip) {
            influxDBClient.enableGzip();
        } else {
            influxDBClient.disableGzip();
        }
    }

    @Override
    public void save(EnhancedRuuviMeasurement measurement) {
        Point point = InfluxDBConverter.toInflux(measurement);
        writeApi.writePoint(point);
        new MqttPublish(measurement);
    }

    @Override
    public void close() {
        influxDBClient.close();
    }
}

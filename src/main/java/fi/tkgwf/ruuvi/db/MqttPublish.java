package fi.tkgwf.ruuvi.db;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;
import fi.tkgwf.ruuvi.config.Config;

public class MqttPublish {

    String MAC;
    String temp;
    String humidity;
    String lastUpdated;
    String rssi;

    private static final Logger LOG = Logger.getLogger(MqttPublish.class);

    public MqttPublish(EnhancedRuuviMeasurement measurement) {
        MAC = measurement.getMac();
        temp = measurement.getTemperature().toString();
        humidity = measurement.getHumidity().toString();
        rssi = measurement.getRssi().toString();
        publish();
    }

    public void publish() {
        int qos = 2;
        final String ruuvi = "ruuvi/";

        try (MemoryPersistence persistence = new MemoryPersistence();
                MqttClient theMQTTClient = new MqttClient(Config.getMQTTbrokerURL(), Config.getMQTTclientId(),
                        persistence);) {
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setUserName(Config.getMQTTUsername());
            connOpts.setPassword(Config.getMQTTPassword().toCharArray());
            connOpts.setCleanSession(false);
            theMQTTClient.connect(connOpts);

            MqttMessage messageTemp = new MqttMessage(temp.getBytes());
            messageTemp.setQos(qos);
            theMQTTClient.publish(ruuvi + MAC + "/temperature", messageTemp);

            MqttMessage messageHumidity = new MqttMessage(humidity.getBytes());
            messageHumidity.setQos(qos);
            theMQTTClient.publish(ruuvi + MAC + "/humidity", messageHumidity);

            MqttMessage messageRssi = new MqttMessage(rssi.getBytes());
            messageRssi.setQos(qos);
            theMQTTClient.publish(ruuvi + MAC + "/rssi", messageRssi);

            theMQTTClient.disconnect();

        } catch (NullPointerException | MqttException e) {
            LOG.error("msg " + e.getMessage());
            LOG.error("loc " + e.getLocalizedMessage());
            LOG.error("cause " + e.getCause());
            LOG.error("exception " + e);
            e.printStackTrace();
        }
    }
}
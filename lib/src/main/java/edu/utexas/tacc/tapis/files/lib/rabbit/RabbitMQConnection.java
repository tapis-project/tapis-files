package edu.utexas.tacc.tapis.files.lib.rabbit;

import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;

public class RabbitMQConnection {

    private static ConnectionFactory INSTANCE;
    private static final IRuntimeConfig conf = RuntimeSettings.get();

    public static synchronized ConnectionFactory getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ConnectionFactory();
            INSTANCE.setHost(conf.getRabbitMQHost());
            INSTANCE.setUsername(conf.getRabbitMQUsername());
            INSTANCE.setPassword(conf.getRabbitmqPassword());
            INSTANCE.setVirtualHost(conf.getRabbitMQVHost());
            INSTANCE.setAutomaticRecoveryEnabled(true);
            INSTANCE.setTopologyRecoveryEnabled(true);
            INSTANCE.setRequestedHeartbeat(30);
            INSTANCE.useNio();
        }
        return INSTANCE;
    }
}

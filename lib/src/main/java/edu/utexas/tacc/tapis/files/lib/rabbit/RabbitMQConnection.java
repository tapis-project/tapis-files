package edu.utexas.tacc.tapis.files.lib.rabbit;

import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;

public class RabbitMQConnection {

    private static ConnectionFactory INSTANCE;
    private static IRuntimeConfig conf = RuntimeSettings.get();

    public static synchronized ConnectionFactory getInstance() {
        if (INSTANCE == null) {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUsername(conf.getRabbitMQUsername());
            connectionFactory.setPassword(conf.getRabbitmqPassword());
            connectionFactory.setVirtualHost(conf.getRabbitMQVHost());
            connectionFactory.useNio();
        }
        return INSTANCE;
    }


}

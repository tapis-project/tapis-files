package edu.utexas.tacc.tapis.files.lib.rabbit;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class RabbitMQConnection {

    private static Connection INSTANCE;

    public static synchronized Connection getInstance() throws IOException {
        if (INSTANCE == null) {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                factory.setPassword("dev");
                factory.setUsername("dev");
                factory.setVirtualHost("dev");
                INSTANCE = factory.newConnection();
            } catch (TimeoutException ex) {
                throw new IOException(ex.getMessage());
            }
        }
        return INSTANCE;
    }


}

package edu.utexas.tacc.tapis.files.lib.workers;

import javax.validation.constraints.NotNull;
import java.util.concurrent.ExecutorService;

public class GenericRabbitMQExecutor implements Runnable{

    private String exchangeName;
    private String queueName;
    private ExecutorService executorService;

    public GenericRabbitMQExecutor(@NotNull String exchangeName, @NotNull String queueName) {
        exchangeName = exchangeName;
        queueName = queueName;
    }


    @Override
    public void run() {

    }
}

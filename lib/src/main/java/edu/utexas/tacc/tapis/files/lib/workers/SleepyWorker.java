package edu.utexas.tacc.tapis.files.lib.workers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class SleepyWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SleepyWorker.class);
    private final int id;

    public SleepyWorker(int id) {
        this.id = id;
    }

    @Override
    public void run() {
        try {
            int sleep = new Random().nextInt(5000) + 100;
            Thread.sleep(sleep);
            log.info("Worker: {} --- Task completed in {} seconds", this.id, sleep);
        } catch (Exception ex) {
            log.info(ex.getMessage());
        }
    }
}

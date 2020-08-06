package edu.utexas.tacc.tapis.files.notifications;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.NotificationsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class NoteSender {

    private static final Logger log = LoggerFactory.getLogger(NoteSender.class);

    private static final NotificationsService notificationsService = new NotificationsService();

    public static void main(String[] args) throws Exception{
        Flux.interval(Duration.ofMillis(1000))
            .publishOn(Schedulers.newElastic("sender"))
            .take(100)
            .flatMap(tick->{
                try {
                    notificationsService.sendNotification("dev", "jmeiring", "hello world!");
                    log.info("Sent a damn message");
                } catch (ServiceException ex) {
                    log.error(ex.getLocalizedMessage(), ex);
                    return Flux.empty();
                }
                return Flux.empty();
            }).subscribe(
                m->{log.info(m.toString());}
            );
    }


}

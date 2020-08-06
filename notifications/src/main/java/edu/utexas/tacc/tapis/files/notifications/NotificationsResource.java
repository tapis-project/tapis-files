package edu.utexas.tacc.tapis.files.notifications;


import com.fasterxml.jackson.databind.ObjectMapper;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.services.NotificationsService;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.inject.Inject;
import javax.websocket.OnClose;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.util.HashSet;
import java.util.Set;

@Service
@ServerEndpoint(value = "/notifications", configurator = AppConfig.class)
public class NotificationsResource {

    private static final Logger log = LoggerFactory.getLogger(NotificationsResource.class);
    private final NotificationsService notificationsService;
    private static Set<Session> sessions = new HashSet<>();
    final Scheduler scheduler = Schedulers.newElastic("messages");
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();


    @Inject
    public NotificationsResource(NotificationsService notificationsService) {
        this.notificationsService = notificationsService;
        notificationsService.streamNotifications()
            .subscribeOn(scheduler)
            .subscribe(m->{
                log.info(m.toString());
                for (Session session: sessions) {
                    try {
                        session.getBasicRemote().sendText(mapper.writeValueAsString(m));
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

            });
    }

    @OnOpen
    public void onOpen(Session s) {
        log.info("******************************************************************");
        sessions.add(s);
    }

    @OnClose
    public void onClose(Session s) {
        sessions.remove(s);
    }

}

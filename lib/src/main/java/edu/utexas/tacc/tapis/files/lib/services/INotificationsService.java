package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FilesNotification;
import reactor.core.publisher.Flux;

public interface INotificationsService {
    void sendNotification(String tenantId, String recipient, String message) throws ServiceException;
    Flux<FilesNotification> streamNotifications();
}

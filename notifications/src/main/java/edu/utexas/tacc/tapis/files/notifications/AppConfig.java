package edu.utexas.tacc.tapis.files.notifications;

import javax.websocket.server.ServerEndpointConfig.Configurator;

import edu.utexas.tacc.tapis.files.lib.services.NotificationsService;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;





/**
 * Instantiates WebSocket end-point with a custom injector so that @Inject can be
 * used normally.
 */
public class AppConfig extends Configurator {
    private ServiceLocator serviceLocator;

    public AppConfig() {
        serviceLocator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(serviceLocator, new AbstractBinder() {

            @Override
            protected void configure() {
                bind(NotificationsService.class).to(NotificationsService.class);
                bind(NotificationsResource.class).to(NotificationsResource.class);
                // Add any other bindings you might need

            }
        });
    }

    @Override
    public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException
    {
        return serviceLocator.getService(endpointClass);
    }
}

package edu.utexas.tacc.tapis.files.notifications;

import org.glassfish.tyrus.server.Server;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class NotificationsApp {

    public static void main(String[] args) {
        Server server = new Server("localhost", 8081, "/", null, NotificationsResource.class);
        try {
            server.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("Please press a key to stop the server.\n");
            reader.readLine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            server.stop();
        }
    }
}

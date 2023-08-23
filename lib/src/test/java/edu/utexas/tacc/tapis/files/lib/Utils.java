package edu.utexas.tacc.tapis.files.lib;

import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Random;

public class Utils {
    public static InputStream makeFakeFile(int size){
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        InputStream is = new ByteArrayInputStream(b);
        return is;
    }

    public static void clearSshSessionPoolInstance() {
        try {
            Field f = SshSessionPool.class.getDeclaredField("instance");
            f.setAccessible(true);
            f.set(null, null);
        } catch (NoSuchFieldException | IllegalAccessException ex) {
            System.out.println("Unable to clear ssh session pool instance.  Exception: " + ex);
        }
    }

}

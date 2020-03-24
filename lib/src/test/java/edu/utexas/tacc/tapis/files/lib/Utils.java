package edu.utexas.tacc.tapis.files.lib;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;

public class Utils {
    public static InputStream makeFakeFile(int size){
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        InputStream is = new ByteArrayInputStream(b);
        return is;
    }

}

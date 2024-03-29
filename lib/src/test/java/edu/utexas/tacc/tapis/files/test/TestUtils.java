package edu.utexas.tacc.tapis.files.test;

import com.fasterxml.jackson.core.type.TypeReference;
import edu.utexas.tacc.tapis.shared.utils.TapisObjectMapper;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class TestUtils {
    private static int CHUNK_MAX = 10240;
    public static TapisSystem readSystem(String configPath, String systemName) throws IOException {
        Map<String, TapisSystem> systemMap = readSystems(configPath);
        return systemMap.get(systemName);
    }

    public static Map<String, TapisSystem> readSystems(String configPath) throws IOException {
        return TapisObjectMapper.getMapper().readValue(TestUtils.class.getClassLoader().getResourceAsStream(configPath),
                    new TypeReference<Map<String, TapisSystem>>() {
                    });
    }

    public static String hashAsHex(byte[] hashBytes) {
        StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
        for (int i = 0; i < hashBytes.length; i++) {
            String hex = Integer.toHexString(0xff & hashBytes[i]);
            if(hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return "sha256:" + hexString.toString();
    }

    public static String getDigest(byte[] bytes) throws NoSuchAlgorithmException, IOException {
        return getDigest(new ByteArrayInputStream(bytes));
    }

    public static String getDigest(InputStream inputStream) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");


        long totalBytesRead = 0;
        int bytesRead = 0;
        byte[] chunk = new byte[CHUNK_MAX];
        do {
            bytesRead = inputStream.read(chunk);
            if(bytesRead > 0) {
                totalBytesRead += bytesRead;
                digest.update(chunk, 0, bytesRead);
            }
        } while (bytesRead > 0);

        return hashAsHex(digest.digest());
    }

}

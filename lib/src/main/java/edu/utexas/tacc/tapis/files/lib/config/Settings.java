package edu.utexas.tacc.tapis.files.lib.config;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 *
 */
public class Settings {

    private Map<String, String> props = System.getProperties().entrySet().stream()
        .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));
    private Map<String, String> envs = System.getenv();
    private Map<String, String> runtimeSettings;


    /**
     * @param key
     * @return value
     */
    public String get(String key) {
        if (runtimeSettings == null) {
            runtimeSettings = new HashMap<>();
            runtimeSettings.putAll(envs);
            runtimeSettings.putAll(props);
        }
        return runtimeSettings.get(key);
    }


}

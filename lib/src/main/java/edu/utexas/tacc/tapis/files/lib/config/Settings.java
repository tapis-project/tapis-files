package edu.utexas.tacc.tapis.files.lib.config;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Get all the environment variables and system properties add them together in a HashMap. System
 * properties override environment variables.
 */
public class Settings {

    private Map<String, String> props = System.getProperties().entrySet().stream()
        .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));
    private Map<String, String> envs = System.getenv();
    private Map<String, String> runtimeSettings;


    /**
     * @param key
     * @param def String default
     * @return value
     */
    public String get(String key, String def) {
        if (runtimeSettings == null) {
            runtimeSettings = new HashMap<>();
            runtimeSettings.putAll(envs);
            runtimeSettings.putAll(props);
        }
        return runtimeSettings.getOrDefault(key, def);
    }

    public String get(String key) {
        if (runtimeSettings == null) {
            runtimeSettings = new HashMap<>();
            runtimeSettings.putAll(envs);
            runtimeSettings.putAll(props);
        }
        return runtimeSettings.get(key);
    }


}

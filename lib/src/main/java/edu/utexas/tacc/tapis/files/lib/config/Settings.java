package edu.utexas.tacc.tapis.files.lib.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.PropertyPermission;

public class Settings {

  private static Map<String, String> props = (Map) System.getProperties();
  private static Map<String, String> envs = System.getenv();
  private static Map<String, String> settings;



  public static String get(String key) {
    if (settings == null) {
      settings = new HashMap();
      settings.putAll(envs);
      settings.putAll(props);
    }
    return settings.get(key);

  }


}

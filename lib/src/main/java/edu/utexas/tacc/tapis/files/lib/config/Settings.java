package edu.utexas.tacc.tapis.files.lib.config;

import java.util.HashMap;
import java.util.Map;


/**
 *
 */
public class Settings {

  private Map props = (Map) System.getProperties();
  private Map<String, String> envs = System.getenv();
  private Map<String, String> runtimeSettings;


  /**
   * @param key
   * @return value
   */
  public String get(String key) {
    if (runtimeSettings == null) {
      runtimeSettings = new HashMap();
      runtimeSettings.putAll(envs);
      runtimeSettings.putAll(props);
    }
    return runtimeSettings.get(key);
  }


}

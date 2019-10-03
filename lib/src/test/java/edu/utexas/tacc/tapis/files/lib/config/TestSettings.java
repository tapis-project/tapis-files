package edu.utexas.tacc.tapis.files.lib.config;

import org.mockito.*;
import org.testng.Assert;
import org.mockito.Mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


//class WTF {
//
//  private static Map<String, String> envars = new HashMap<>();
//  private static Map<String, String> props = new HashMap<>();
//
//  public static String getenv(String key) {
//    return envars.get(key);
//  }
//
//  public static String getProperty(String key) {
//    return props.get(key);
//  }
//
//  public static String setProperty(String key, String val) {
//    return props.put(key, val);
//  }
//
//  public static String setEnv(String key, String val) {
//    return envars.put(key, val);
//  }
//
//}



@Test
public class TestSettings {

  @Mock
  List<String> mockedList;

  @Mock
  Settings settings;

  @BeforeMethod(alwaysRun=true)
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void whenUseMockAnnotation_thenMockIsInjected() {

    mockedList.add("one");
    Mockito.verify(mockedList).add("one");
    Assert.assertEquals(0, mockedList.size());

    Mockito.when(mockedList.size()).thenReturn(100);
    Assert.assertEquals(100, mockedList.size());
  }
}

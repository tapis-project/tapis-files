package edu.utexas.tacc.tapis.files.lib.clients;


// TODO: Replace with system definition from Systems Service SDK
public class FakeSystem {

  private String host;
  private Long port;
  private String username;
  private String password;
  private String protocol;
  private String bucketName;

  public FakeSystem (String host, Long port, String username, String password, String protocol, String bucketName) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.protocol = protocol;
    this.bucketName = bucketName;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Long getPort() {
    return port;
  }

  public void setPort(Long port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }
}

package edu.utexas.tacc.tapis.files.lib.clients;



// TODO: Replace with system definition from Systems Service SDK
public class FakeSystem {

  public enum AccessMechanism {NONE, SSH_ANONYMOUS, SSH_PASSWORD, SSH_KEYS, SSH_CERT};
  private String host;
  private Long port;
  private String username;
  private String password;
  private String protocol;
  private String rootDir;
  private String bucketName;
  private AccessMechanism accessMechanism;
 

  public FakeSystem (String host, Long port, String username, String password, String protocol, String rootDir, String bucketName, AccessMechanism accessMechanism) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.protocol = protocol;
    this.rootDir = rootDir;
    this.bucketName = bucketName;
    this.accessMechanism = accessMechanism;
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
  
  public String getAccessMechanism() {
	  return accessMechanism.name();
  }
  public void setAccessMechanism(AccessMechanism accessMechanism) {
	  this.accessMechanism = accessMechanism;
  }

public String getRootDir() {
	return rootDir;
}

public void setRootDir(String rootDir) {
	this.rootDir = rootDir;
}
}

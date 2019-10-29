package edu.utexas.tacc.tapis.files.lib.clients;

public class FakeSystemsService {

  public FakeSystem getSystemByID(String systemId) {
    return  new FakeSystem(
        "http://localhost",
        9000L,
        "user",
        "password",
        "S3",
        "test"
    );
  }
}

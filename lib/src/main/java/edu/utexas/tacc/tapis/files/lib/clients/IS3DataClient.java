package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;

public interface IS3DataClient extends IRemoteDataClient
{
  void makeBucket(String name) throws IOException;
}

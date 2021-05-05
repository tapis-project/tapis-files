package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;

public interface IS3DataClient extends IRemoteDataClient
{
  // In addition to oboTenant, oboUser and systemId in IRemoteDataClient an S3Client also has an associated bucket
  String getBucket();

  void makeBucket(String name) throws IOException;
}

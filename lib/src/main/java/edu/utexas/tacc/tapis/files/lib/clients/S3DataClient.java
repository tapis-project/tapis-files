package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.utils.S3URLParser;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;

/**
 * This class provides remoteDataClient file operations for S3 systems.
 * Note that S3 buckets do not have a hierarchical structure. There are no directories.
 * Everything is an object associated with a key.
 * Note that in Tapis the difference between a key and an absolute path is the path starts with "/"
 *   and the key does not.
 */
public class S3DataClient implements IS3DataClient
{
    private final Logger log = LoggerFactory.getLogger(S3DataClient.class);

    public String getOboTenant() { return oboTenant; }
    public String getOboUser() { return oboUser; }
    public String getSystemId() { return system.getId(); }
    public String getBucket() { return bucket; }

    private final String oboTenant;
    private final String oboUser;

    public S3Client getClient() {
        return client;
    }

    private final S3Client client;
    private final String bucket;
    private final TapisSystem system;
    private final String rootDir;

    public S3DataClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull TapisSystem system1)
            throws IOException
    {
      oboTenant = oboTenant1;
      oboUser = oboUser1;
      system = system1;
      bucket = system.getBucketName();
      // Make sure we have a valid rootDir that is not empty and begins with /
      String tmpDir = system.getRootDir();
      if (StringUtils.isBlank(tmpDir)) tmpDir = "/";
      rootDir = StringUtils.prependIfMissing(tmpDir,"/");

      // There are so many flavors of s3 URLs we have to do the gymnastics below.
      try {
        String host = system.getHost();
        String region = S3URLParser.getRegion(host);
        URI endpoint = configEndpoint(host);
        Region reg;

        //For minio/other S3 compliant APIs, the region is not needed
        if (region == null) reg = Region.US_EAST_1;
        else reg = Region.of(region);

        String accessKey = "";
        String accessSecret = "";
        if (system.getAuthnCredential() != null)
        {
          accessKey = system.getAuthnCredential().getAccessKey();
          accessSecret = system.getAuthnCredential().getAccessSecret();
        }
        AwsCredentials credentials = AwsBasicCredentials.create(accessKey, accessSecret);
        S3ClientBuilder builder = S3Client.builder()
                .region(reg)
                .credentialsProvider(StaticCredentialsProvider.create(credentials));

        // Have to do the endpoint override if its not a real AWS route, as in the case for a minio instance
        if (!S3URLParser.isAWSUrl(host)) builder.endpointOverride(endpoint);
        client = builder.build();

      } catch (URISyntaxException e) {
        String msg = Utils.getMsg("FILES_CLIENT_S3_ERR", oboTenant, oboUser, system.getId(), bucket, e.getMessage());
        log.error(msg);
        throw new IOException(msg, e);
      }
    }

  /**
   * Build a URI using host, scheme, port
   *
   * @param host Host from the System
   * @return a URI
   * @throws URISyntaxException on error
   */
    public URI configEndpoint(String host) throws URISyntaxException
    {
      URI endpoint;
      URI tmpURI = new URI(host);
      // Build a URI setting host, scheme, port
      UriBuilder uriBuilder = UriBuilder.fromUri("");
      uriBuilder.host(tmpURI.getHost()).scheme(tmpURI.getScheme());
      if ((system.getPort() != null) && (system.getPort() > 0)) uriBuilder.port(system.getPort());
      if (StringUtils.isBlank(tmpURI.getHost())) uriBuilder.host(host);
      //Make sure there is a scheme, and default to https if not.
      if (StringUtils.isBlank(tmpURI.getScheme())) uriBuilder.scheme("https");
      endpoint = uriBuilder.build();
      return endpoint;
    }

  /**
   * Create a bucket
   * @param name - name of bucket
   */
  @Override
  public void makeBucket(String name)
  {
    CreateBucketRequest req = CreateBucketRequest.builder().bucket(name).build();
    client.createBucket(req);
  }

  /**
   * Return all S3 objects matching a path prefix using default max limit and 0 offset
   *
   * @param path - Path to file or directory relative to the system rootDir
   * @return list of FileInfo objects
   * @throws IOException       Generally a network error
   * @throws NotFoundException No file at target
   */
  public List<FileInfo> ls(@NotNull String path) throws IOException, NotFoundException
  {
    return ls(path, MAX_LISTING_SIZE, 0);
  }

  /**
   * Return all S3 objects matching a path prefix
   * NOTE that although we take limit as type long the S3 iterator takes an int so max is Integer.MAX_VALUE
   *
   * @param path - Path to file or directory relative to the system rootDir
   * @param limit - maximum number of keys to return
   * @param offset - Offset for listing
   * @return list of FileInfo objects
   * @throws IOException       Generally a network error
   * @throws NotFoundException No file at target
   */
    @Override
    public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws IOException, NotFoundException
    {
      int maxKeys = Integer.MAX_VALUE;
      if (limit < Integer.MAX_VALUE) maxKeys = (int) limit;

      String absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();

      Stream<S3Object> response = listWithIterator(absolutePath, maxKeys);
      List<FileInfo> files = new ArrayList<>();
      response.skip(offset).limit(limit).forEach((S3Object x) -> files.add(new FileInfo(x)));

      // For s3 at least, if the listing is empty it could just be not found, which should really throw
      // a NotFoundException
      if (files.isEmpty()) doesExist(absolutePath);
      return files;
    }

  /** UNSUPPORTED
   * Create a simulated "directory" in S3
   * Note that this is simply an S3 key that always ends with a /
   *
   * @param path - Path to directory relative to the system rootDir
   * @throws IOException Generally a network error
   */
    @Override
    public void mkdir(@NotNull String path) throws IOException
    {
      String msg = Utils.getMsg("FILES_CLIENT_S3_NO_SUPPORT", oboTenant, oboUser, "mkdir", system.getId(), bucket, path);
      throw new NotImplementedException(msg);
//      String pathAsDir = PathUtils.getAbsolutePath(rootDir, path).toString();
//      // Since absolute paths do not have a trailing / we will add one in order to support the concept of a directory
//      pathAsDir = StringUtils.appendIfMissing(pathAsDir, "/");
//      // Create an empty object associated with this "directory" key
//      try {
//        PutObjectRequest req = PutObjectRequest.builder().bucket(bucket).key(pathAsDir).build();
//        client.putObject(req, RequestBody.fromString(""));
//      } catch (S3Exception ex) {
//        String msg = Utils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "mkdir", system.getId(), bucket,
//                                   path, ex.getMessage());
//        log.error(msg);
//        throw new IOException(msg, ex);
//      }
    }

  /**
   * Upload an S3 object
   *
   * @param path - Path to object relative to the system rootDir
   * @param fileStream Stream of data to place in object
   * @throws IOException on error
   */
  @Override
  public void upload(@NotNull String path, @NotNull InputStream fileStream) throws IOException
  {
    // TODO: This should use multipart on an InputStream ideally;
    // Determine the absolute path and the corresponding object key.
    String absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();
    String objKey = StringUtils.removeStart(absolutePath, "/");
    File scratchFile = File.createTempFile(UUID.randomUUID().toString(), "tmp");
    try {
      FileUtils.copyInputStreamToFile(fileStream, scratchFile);
      PutObjectRequest req = PutObjectRequest.builder().bucket(bucket).key(objKey).build();
      client.putObject(req, RequestBody.fromFile(scratchFile));
    } catch (S3Exception ex) {
      String msg = Utils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "insert", system.getId(), bucket,
                                path, ex.getMessage());
      log.error(msg);
      throw new IOException(msg, ex);
    } finally {
      scratchFile.delete();
    }
  }

  /**
   * Move an S3 object from one key to another
   *
   * @param srcPath current location relative to system rootDir
   * @param dstPath desired location relative to system rootDir
   * @throws IOException Network errors generally
   * @throws NotFoundException Source path not found
   */
  @Override
  public void move(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException
  {
    // Determine the absolute srcPath and the corresponding object key.
    String srcAbsolutePath = PathUtils.getAbsolutePath(rootDir, srcPath).toString();
    String srcKey = StringUtils.removeStart(srcAbsolutePath, "/");
    // Make sure the source object exists
    doesExist(srcAbsolutePath);
    // Determine the absolute dstPath and the corresponding object key.
    String dstAbsolutePath = PathUtils.getAbsolutePath(rootDir, dstPath).toString();
    String dstKey = StringUtils.removeStart(dstAbsolutePath, "/");
    copyObject(srcKey, dstKey, true);
  }

    /**
     * Copy an S3 object from one key to another
     *
     * @param srcPath current location relative to system rootDir
     * @param dstPath desired location relative to system rootDir
     * @throws IOException Network errors generally
     * @throws NotFoundException Source path not found
     */
    @Override
    public void copy(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException
    {
      // Determine the absolute srcPath and the corresponding object key.
      String srcAbsolutePath = PathUtils.getAbsolutePath(rootDir, srcPath).toString();
      String srcKey = StringUtils.removeStart(srcAbsolutePath, "/");
      // Make sure the source object exists
      doesExist(srcAbsolutePath);
      // Determine the absolute dstPath and the corresponding object key.
      String dstAbsolutePath = PathUtils.getAbsolutePath(rootDir, dstPath).toString();
      String dstKey = StringUtils.removeStart(dstAbsolutePath, "/");
      copyObject(srcKey, dstKey, false);
    }

  /**
   * Delete either a single object or all objects under relative path "/"
   *
   * @param path - Path to object relative to the system rootDir
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
    @Override
    public void delete(@NotNull String path) throws IOException, NotFoundException
    {
      // Determine the absolute path and the corresponding object key.
      String absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();
      String objKey = StringUtils.removeStart(absolutePath, "/");
      // If relative path is "/" delete all objects in rootDir
      // else remove a single object
      if (StringUtils.isBlank(path) || "/".equals(path))
      {
        // We are removing all from rootDir, figure out the key prefix
        absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();
        objKey = StringUtils.removeStart(absolutePath, "/");
        // If rootDir is / we now have an empty key, use / as the key.
        if (StringUtils.isBlank(objKey)) objKey = "/";
        deleteAll(objKey);
      }
      else
      {
        // Remove a single object
        try { deleteObject(objKey); }
        catch (NoSuchKeyException ex) { throw new NotFoundException(); }
        catch (S3Exception ex) {
          String msg = Utils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "delete", system.getId(), bucket,
                  path, ex.getMessage());
          log.error(msg);
          throw new IOException(msg, ex);
        }
      }
    }

  @Override
  public FileInfo getFileInfo(@NotNull String path) throws IOException
  {
    FileInfo fileInfo = null;
    try
    {
      String absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();
      Stream<S3Object> response = listWithIterator(absolutePath, 1);
      List<FileInfo> files = new ArrayList<>();
      response.limit(1).forEach((S3Object x) -> files.add(new FileInfo(x)));
      if (!files.isEmpty()) fileInfo = files.get(0);
    }
    catch (NoSuchKeyException ex) { fileInfo = null; }
    catch (S3Exception ex)
    {
      String msg = Utils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "delete", system.getId(), bucket,
                                path, ex.getMessage());
      throw new IOException(msg, ex);
    }
    return fileInfo;
  }

  @Override
  public InputStream getStream(@NotNull String path) throws IOException, NotFoundException
  {
    // Determine the absolute path and the corresponding object key.
    String absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();
    String objKey = StringUtils.removeStart(absolutePath, "/");
    try
    {
      GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(objKey).build();
      return client.getObject(req, ResponseTransformer.toInputStream());
    }
    catch (NoSuchKeyException ex)
    {
      throw new NotFoundException();
    }
    catch (S3Exception ex)
    {
      String msg = Utils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "getStream", system.getId(), bucket,
                                path, ex.getMessage());
      log.error(msg);
      throw new IOException(msg, ex);
    }
  }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count)
            throws IOException, NotFoundException
    {
      // Determine the absolute path and the corresponding object key.
      String absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();
      String objKey = StringUtils.removeStart(absolutePath, "/");
      try {
        // S3 api includes the final byte, different than posix, so we subtract one to get the proper count.
        String brange = String.format("bytes=%s-%s", startByte, startByte + count - 1);
        GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).range(brange).key(objKey).build();
        return client.getObject(req, ResponseTransformer.toInputStream());
      } catch (NoSuchKeyException ex) {
        throw new NotFoundException();
      } catch (S3Exception ex) {
        String msg = Utils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "getBytesByRange", system.getId(), bucket,
                path, ex.getMessage());
        log.error(msg);
        throw new IOException(msg, ex);
      }
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte)
    {
      String msg = Utils.getMsg("FILES_CLIENT_S3_NO_SUPPORT", oboTenant, oboUser, "putBytesByRange", system.getId(), bucket, path);
      throw new NotImplementedException(msg);
    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream)
    {
      String msg = Utils.getMsg("FILES_CLIENT_S3_NO_SUPPORT", oboTenant, oboUser, "append", system.getId(), bucket, path);
      throw new NotImplementedException(msg);
    }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Copy an object with the option to delete the old key
   *
   * @param srcKey S3 key of object to copy
   * @param dstKey desired location relative to system rootDir
   * @param withDelete flag indicating if old object should be removed
   * @throws IOException on error
   */
  private void copyObject(@NotNull String srcKey, @NotNull String dstKey, boolean withDelete) throws IOException
  {
    doCopy(srcKey, dstKey);
    if (withDelete) delete(srcKey);
  }

  /**
   * Copy an object to a new key
   * @param srcKey S3 key of object to copy
   * @param dstKey new path for the object relative to the system rootDir
   * @throws IOException on error
   */
  private void doCopy(@NotNull String srcKey, @NotNull String dstKey) throws NotFoundException, IOException
  {
    // Source path encoded as a bucket URL
    String bucketSrcKey = bucket + "/" + srcKey;
    CopyObjectRequest req = CopyObjectRequest.builder()
            .destinationBucket(bucket)
            .copySource(bucketSrcKey)
            .destinationKey(dstKey)
            .build();
    try
    {
      client.copyObject(req);
    }
    catch (NoSuchKeyException ex) { throw new NotFoundException(); }
    catch (S3Exception ex)
    {
      String msg = Utils.getMsg("FILES_CLIENT_S3_OP_ERR3", oboTenant, oboUser, "doCopy", system.getId(), bucket,
                                srcKey, dstKey, bucketSrcKey, dstKey, ex.getMessage());
      log.error(msg);
      throw new IOException(msg, ex);
    }
  }

  /**
   * For the System bucket and provided path prefix list all matching S3 objects
   *
   * @param objPrefixPath prefix path
   * @param maxKeys maximum number of keys to return, null for no limit
   * @return stream of s3 objects
   */
  private Stream<S3Object> listWithIterator(String objPrefixPath, Integer maxKeys)
  {
    ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(objPrefixPath);
    if (maxKeys != null) reqBuilder.maxKeys(maxKeys);
    ListObjectsV2Request req = reqBuilder.build();
    ListObjectsV2Iterable resp = client.listObjectsV2Paginator(req);
    return resp.contents().stream();
  }

  /**
   * Check to see if an S3 object exists.
   * @param objKey - Object key
   * @throws NotFoundException if key does not exist
   */
  private void doesExist(String objKey) throws NotFoundException
  {
    // Head always exists (plus cannot do headObject() on root of the bucket
    if (StringUtils.isBlank(objKey)) return;
    try {
      HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucket).key(objKey).build();
      client.headObject(req);
    }
    catch (NoSuchKeyException ex) {
      String msg = Utils.getMsg("FILES_CLIENT_S3_NOFILE", oboTenant, oboUser, system.getId(), bucket, objKey);
      log.error(msg);
      throw new NotFoundException(msg);
    }
  }

  /**
   * Delete S3 object with associated key
   * @param objKey - S3 object key
   * @throws S3Exception on error
   */
  private void deleteObject(String objKey) throws S3Exception
  {
    DeleteObjectRequest req = DeleteObjectRequest.builder().bucket(bucket).key(objKey).build();
    client.deleteObject(req);
  }

  /**
   * Delete all S3 objects matching the provided prefix
   * @param objKeyPrefix - S3 object key prefix
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
  private void deleteAll(@NotNull String objKeyPrefix) throws IOException, NotFoundException
  {
    try
    {
      // List all objects under objKeyPrefix, deleting each one as we go
      listWithIterator(objKeyPrefix, null).forEach(object -> {
        try { deleteObject(object.key()); }
        catch (S3Exception ex) { /* Ignore exception from individual operation, delete as much as possible */ }
      });
    }
    catch (NoSuchKeyException ex) { throw new NotFoundException(); }
    catch (S3Exception ex) {
      String msg = Utils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "delete", system.getId(), bucket,
              objKeyPrefix, ex.getMessage());
      log.error(msg);
      throw new IOException(msg, ex);
    }
  }
}

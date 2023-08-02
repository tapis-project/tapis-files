package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.UriBuilder;
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
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import edu.utexas.tacc.tapis.shared.s3.S3Utils;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;

/**
 * This class provides remoteDataClient file operations for S3 systems.
 *
 * Note that S3 buckets do not have a hierarchical structure. There are no directories.
 * Everything is an object associated with a key.
 *
 * In S3 Tapis support, the difference between a key and an absolute path is the absolute path starts with a "/"
 *   and the key does not. So for the final fully resolved key to the object the absolute path is used with the
 *   initial "/" stripped off.
 *
 * Tapis does not support S3 keys beginning with a "/". Such objects may be shown in a listing, but it will not be
 *   possible through Tapis to reference the object directly using the key.
 *
 * Note that for S3 this means a path of "/" or the empty string indicates all objects in the bucket with a prefix matching
 *   *rootDir*, with any preceding "/" stripped off of *rootDir*
 *
 */
public class S3DataClient implements IRemoteDataClient
{
  private final Logger log = LoggerFactory.getLogger(S3DataClient.class);
  private final String oboTenant;
  private final String oboUser;
  private final S3Client client;
  private final String bucket;
  private final TapisSystem system;
  private final String rootDir;

  @Override
  public String getOboTenant() { return oboTenant; }
  @Override
  public String getOboUser() { return oboUser; }
  @Override
  public String getSystemId() { return system.getId(); }
  @Override
  public SystemTypeEnum getSystemType() { return system.getSystemType(); }
  @Override
  public TapisSystem getSystem() { return system; }

  public S3Client getClient() { return client; }

  public S3DataClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull TapisSystem system1)
          throws IOException
  {
    oboTenant = oboTenant1;
    oboUser = oboUser1;
    system = system1;
    bucket = system.getBucketName();
    // Make sure we have a valid rootDir that is not null and does not have extra whitespace
    rootDir = (StringUtils.isBlank(system.getRootDir())) ? "" :  system.getRootDir();

    // There are so many flavors of s3 URLs we have to do the gymnastics below.
    try
    {
      String host = system.getHost();
      int port = system.getPort() == null ? -1 : system.getPort();
      String region = S3Utils.getS3Region(host);
      URI endpoint = configEndpoint(host, port);
      Region reg;

      //For minio/other S3 compliant APIs, the region is not needed
      if (region == null) reg = Region.US_EAST_1;
      else reg = Region.of(region);

      // If we do not have credentials it is an error
      if (system.getAuthnCredential() == null)
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_S3_ERR", oboTenant, oboUser, system.getId(), bucket, "No credentials");
        log.warn(msg);
        throw new IOException(msg);
      }
      String accessKey = system.getAuthnCredential().getAccessKey();
      String accessSecret = system.getAuthnCredential().getAccessSecret();
      // If access key or secret is blank it is an error
      if (StringUtils.isBlank(accessKey))
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_S3_ERR", oboTenant, oboUser, system.getId(), bucket, "Blank accessKey");
        log.warn(msg);
        throw new IOException(msg);
      }
      if (StringUtils.isBlank(accessSecret))
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_S3_ERR", oboTenant, oboUser, system.getId(), bucket, "Blank accessSecret");
        log.warn(msg);
        throw new IOException(msg);
      }
      AwsCredentials credentials = null;
      // We catch Exception here because AwsBasicCredentials.create() throws various exceptions.
      try
      {
        credentials = AwsBasicCredentials.create(accessKey, accessSecret);
      }
      catch (Exception e)
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_S3_ERR", oboTenant, oboUser, system.getId(), bucket, e.getMessage());
        log.warn(msg);
        throw new IOException(msg);
      }
      // If AWS returned null for credentials we cannot go on
      if (credentials == null)
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_S3_ERR", oboTenant, oboUser, system.getId(), bucket,
                                    "AwsBasicCredentials.create returned null");
        log.warn(msg);
        throw new IOException(msg);
      }
      S3ClientBuilder builder = S3Client.builder()
              .region(reg)
              .credentialsProvider(StaticCredentialsProvider.create(credentials));

      // Have to do the endpoint override if it is not a real AWS route, as in the case for a minio instance
      if (!S3Utils.isAWSUrl(host))
      {
        log.debug(LibUtils.getMsg("FILES_CLIENT_S3_EP_OVER", oboTenant, oboUser, system.getId(), bucket,
                reg.toString(), host, endpoint.toString()));
        builder.endpointOverride(endpoint);
      }
      // Log info about client we are building
      log.debug(LibUtils.getMsg("FILES_CLIENT_S3_BUILD", oboTenant, oboUser, system.getId(), bucket,
              reg.toString(), host, endpoint.toString()));
      // Build the client
      client = builder.build();
    }
    catch (Exception e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_S3_ERR", oboTenant, oboUser, system.getId(), bucket, e.getMessage());
      log.error(msg);
      throw new IOException(msg, e);
    }
    log.debug(LibUtils.getMsg("FILES_CLIENT_S3_BUILT", oboTenant, oboUser, system.getId(), bucket));
  }

  /**
   * Build a URI using host, scheme, port
   *
   * @param host Host from the System
   * @param port port from the System
   * @return a URI
   * @throws URISyntaxException on error
   */
  public URI configEndpoint(String host, int port) throws URISyntaxException
  {
    URI tmpURI = new URI(host);
    // Build a URI setting host, scheme, port
    UriBuilder uriBuilder = UriBuilder.fromUri("");
    uriBuilder.host(tmpURI.getHost()).scheme(tmpURI.getScheme());
    if (port > 0) uriBuilder.port(port);
    if (StringUtils.isBlank(tmpURI.getHost())) uriBuilder.host(host);
    //Make sure there is a scheme, and default to https if not.
    if (StringUtils.isBlank(tmpURI.getScheme())) uriBuilder.scheme("https");
    return uriBuilder.build();
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
   * @throws IOException Generally a network error
   * @throws NotFoundException No file at target
   */
  @Override
  public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws IOException, NotFoundException
  {
    int maxKeys = Integer.MAX_VALUE;
    if (limit < Integer.MAX_VALUE) maxKeys = (int) limit;

    String absoluteKey = PathUtils.getAbsoluteKey(rootDir, path);

    Stream<S3Object> response = listWithIterator(absoluteKey, maxKeys);
    List<FileInfo> files = new ArrayList<>();
    response.skip(offset).limit(limit).forEach((S3Object x) -> files.add(new FileInfo(x, system.getId(), rootDir)));

    // For s3 at least, if the listing is empty it could just be not found, which should really throw NotFoundException
    if (files.isEmpty()) doesExist(absoluteKey);
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
    String msg = LibUtils.getMsg("FILES_CLIENT_S3_NO_SUPPORT", oboTenant, oboUser, "mkdir", system.getId(), bucket, path);
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
    String objKey = PathUtils.getAbsoluteKey(rootDir, path);
    File scratchFile = File.createTempFile(UUID.randomUUID().toString(), "tmp");
    try
    {
      FileUtils.copyInputStreamToFile(fileStream, scratchFile);
      PutObjectRequest req = PutObjectRequest.builder().bucket(bucket).key(objKey).build();
      client.putObject(req, RequestBody.fromFile(scratchFile));
    }
    catch (S3Exception ex)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "insert", system.getId(), bucket,
              path, ex.getMessage());
      log.error(msg);
      throw new IOException(msg, ex);
    }
    finally
    {
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
    String srcKey = PathUtils.getAbsoluteKey(rootDir, srcPath);
    // Make sure the source object exists
    doesExist(srcKey);
    // Determine the absolute dstPath and the corresponding object key.
    String dstKey = PathUtils.getAbsoluteKey(rootDir, dstPath);
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
    String srcKey = PathUtils.getAbsoluteKey(rootDir, srcPath);
    // Make sure the source object exists
    doesExist(srcKey);
    // Determine the absolute dstPath and the corresponding object key.
    String dstKey = PathUtils.getAbsoluteKey(rootDir, dstPath);
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
    String objKey = PathUtils.getAbsoluteKey(rootDir, path);
    // If relative path is "" delete all objects in rootDir
    // else remove a single object
    if (StringUtils.isEmpty(objKey)) { deleteAllObjectsInBucket(); }
    else
    {
      // Remove a single object
      try { deleteObject(objKey); }
      catch (NoSuchKeyException ex) { throw new NotFoundException(); }
      catch (S3Exception ex)
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "delete", system.getId(), bucket,
                                     path, ex.getMessage());
        log.error(msg);
        throw new IOException(msg, ex);
      }
    }
  }

  @Override
  public FileInfo getFileInfo(@NotNull String path, boolean followLinks) throws IOException
  {
    FileInfo fileInfo = null;
    try
    {
      String absoluteKey = PathUtils.getAbsoluteKey(rootDir, path);
      Stream<S3Object> response = listWithIterator(absoluteKey, 1);
      List<FileInfo> files = new ArrayList<>();
      response.limit(1).forEach((S3Object x) -> files.add(new FileInfo(x, system.getId(), rootDir)));
      if (!files.isEmpty()) fileInfo = files.get(0);
    }
    catch (NoSuchKeyException ex) { fileInfo = null; }
    catch (S3Exception ex)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "delete", system.getId(), bucket,
              path, ex.getMessage());
      throw new IOException(msg, ex);
    }
    return fileInfo;
  }

  @Override
  public InputStream getStream(@NotNull String path) throws IOException, NotFoundException
  {
    // Determine the absolute path and the corresponding object key.
    String objKey = PathUtils.getAbsoluteKey(rootDir, path);
    try
    {
      GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(objKey).build();
      return client.getObject(req, ResponseTransformer.toInputStream());
    }
    catch (NoSuchKeyException ex)
    {
      throw new NotFoundException(String.format("Object key not found: %s",objKey));
    }
    catch (S3Exception ex)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "getStream", system.getId(), bucket,
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
    String objKey = PathUtils.getAbsoluteKey(rootDir, path);
    try {
      // S3 api includes the final byte, different than posix, so we subtract one to get the proper count.
      String brange = String.format("bytes=%s-%s", startByte, startByte + count - 1);
      GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).range(brange).key(objKey).build();
      return client.getObject(req, ResponseTransformer.toInputStream());
    } catch (NoSuchKeyException ex) {
      throw new NotFoundException();
    } catch (S3Exception ex) {
      String msg = LibUtils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "getBytesByRange", system.getId(), bucket,
              path, ex.getMessage());
      log.error(msg);
      throw new IOException(msg, ex);
    }
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Copy an object with the option to delete the old key
   *
   * @param srcKey S3 absolute source key of object to copy
   * @param dstKey desired absolute destination key
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
    CopyObjectRequest req = CopyObjectRequest.builder()
            .sourceBucket(bucket).sourceKey(srcKey)
            .destinationBucket(bucket).destinationKey(dstKey)
            .build();
    try
    {
      client.copyObject(req);
    }
    catch (NoSuchKeyException ex) { throw new NotFoundException(); }
    catch (S3Exception ex)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_S3_OP_ERR3", oboTenant, oboUser, "doCopy", system.getId(), bucket,
                                   srcKey, dstKey, srcKey, dstKey, ex.getMessage());
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
      String msg = LibUtils.getMsg("FILES_CLIENT_S3_NOFILE", oboTenant, oboUser, system.getId(), bucket, objKey);
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
   * Delete all S3 objects in the bucket
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
  private void deleteAllObjectsInBucket() throws IOException, NotFoundException
  {
    String objKeyPrefix = "";
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
      String msg = LibUtils.getMsg("FILES_CLIENT_S3_OP_ERR1", oboTenant, oboUser, "delete", system.getId(), bucket,
              objKeyPrefix, ex.getMessage());
      log.error(msg);
      throw new IOException(msg, ex);
    }
  }
}

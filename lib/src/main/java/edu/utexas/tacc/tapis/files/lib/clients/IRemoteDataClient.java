package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import javax.ws.rs.NotFoundException;

public interface IRemoteDataClient
{
  // A remote data client always has at a minimum an associated oboUser, oboTenant and systemId
  String getOboTenant();
  String getOboUser();
  String getSystemId();

  /**
   * Record a reservation for a client. Only used for SSH clients.
   * A reservation indicates a service call using the client is in progress.
   * A client should not be fully closed until all reservations are released even if the client has
   *   already been removed from the SSHConnectionCache until
   */
  void reserve();

  /**
   * Release a reservation for a client
   */
  void release();

  /**
   * List files or objects at a path
   *
   * @param path - path on system relative to system rootDir
   * @param limit - Max number of items to return
   * @param offset - offset
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
  List<FileInfo> ls(@NotNull String path, long limit, long offset) throws NotFoundException, IOException;
  List<FileInfo> ls(@NotNull String path) throws NotFoundException, IOException;

  /**
   * Upload will place the entire contents of an InputStream to the location at remotePath.
   *
   * @param path - path on system relative to system rootDir
   * @param fileStream - the input stream
   * @throws IOException on error
   */
  void upload(@NotNull String path, @NotNull InputStream fileStream) throws IOException;

  /**
   * Create a directory.
   * NOTE: Not supported for all system types.
   *
   * @param path - path on system relative to system rootDir
   * @throws IOException on error
   */
  void mkdir(@NotNull  String path) throws IOException;

  /**
   * Move a file or object from one path to another.
   *
   * @param srcPath - source path on system relative to system rootDir
   * @param dstPath - destination path on system relative to system rootDir
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
  void move(@NotNull String srcPath, @NotNull String dstPath) throws NotFoundException, IOException;

  /**
   * Copy a file or object from one path to another.
   *
   * @param srcPath - source path on system relative to system rootDir
   * @param dstPath - destination path on system relative to system rootDir
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
  void copy(@NotNull String srcPath, @NotNull String dstPath) throws NotFoundException, IOException;

  /**
   * Delete a file or object
   *
   * @param path - path on system relative to system rootDir
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
  void delete(@NotNull String path) throws NotFoundException, IOException;

  /**
   * Returns file info for the file/dir or object if path exists, null if path does not exist
   *
   * @param path - path on system relative to system rootDir
   * @return FileInfo for the object or null if path does not exist.
   * @throws IOException on error
   */
  FileInfo getFileInfo(@NotNull String path) throws IOException;

  /**
   * Returns a stream of the entire contents of a file or object.
   * @param path - path on system relative to system rootDir
   * @return stream of data as input stream
   * @throws IOException on error
   */
  InputStream getStream(@NotNull String path) throws IOException;

  /**
   *
   * @param path - path on system relative to system rootDir
   * @param startByte position of first byte to return
   * @param count Number of bytes returned
   * @return InputStream
   * @throws IOException Generic IO Exception
   */
  InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException;

  /**
   * Not many fielsystems and/or file formats support this type of operation
   * @param path - path on system relative to system rootDir
   * @param byteStream stream of data to put
   * @param startByte start point
   * @param endByte end point
   * @throws IOException on error
   */
  void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException;

  /**
   * Append will take an existing file at location path and append the byteStream to the end of it.
   * @param path - path on system relative to system rootDir
   * @param byteStream input stream
   * @throws IOException on error
   */
  void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException;
}

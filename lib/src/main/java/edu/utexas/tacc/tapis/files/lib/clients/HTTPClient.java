package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.ws.rs.NotFoundException;

import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;

public class HTTPClient implements IRemoteDataClient
{
  private static final Logger log = LoggerFactory.getLogger(HTTPClient.class);

  @Override
  public void reserve() {}
  @Override
  public void release() {}

  @Override
  public String getOboTenant() { return oboTenant; }
  @Override
  public String getOboUser() { return oboUser; }
  @Override
  public SystemTypeEnum getSystemType() { return null; }
  @Override
  public TapisSystem getSystem() { return null; }

  // Since no system use srcDstURIs instead
  @Override
  public String getSystemId() { return srcDstURIs; }

  private final String oboTenant;
  private final String oboUser;
  private final String srcDstURIs;

  public HTTPClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull String srcUri, @NotNull String dstUri) {
      oboTenant = oboTenant1;
      oboUser = oboUser1;
      srcDstURIs = String.format("SRC:%s,DST:%s", srcUri, dstUri);
    }

    @Override
    public List<FileInfo> ls(@NotNull String path) throws IOException, NotFoundException {
        throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "ls"));
    }

    @Override
    public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws IOException, NotFoundException {
        throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "ls") );
    }

    @Override
    public void upload(@NotNull String path, @NotNull InputStream fileStream) throws IOException {
        throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "insert") );
    }

    @Override
    public void mkdir(@NotNull String path) throws IOException {
        throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "mkdir") );
    }

    @Override
    public void move(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException {
        throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "move") );
    }

    @Override
    public void copy(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException {
        throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "copy") );
    }

    @Override
    public void delete(@NotNull String path) throws IOException, NotFoundException {
        throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "delete") );
    }

  @Override
  public FileInfo getFileInfo(@NotNull String path) throws NotFoundException
  {
    throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "getFileInfo") );
  }

  @Override
  public InputStream getStream(@NotNull String path) throws IOException
  {
    OkHttpClient client = new OkHttpClient.Builder().build();
    Request request = new Request.Builder().url(path).build();
    Response response = client.newCall(request).execute();
    if (!response.isSuccessful())
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_HTTP_ERR", oboTenant, oboUser, srcDstURIs, path, response);
      log.error(msg);
      throw new IOException(msg);
    }
    return response.body().byteStream();
  }

   @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        throw new NotImplementedException(LibUtils.getMsg("FILES_CLIENT_HTTP_NOT_IMPL", oboTenant, oboUser, "") );
    }
}

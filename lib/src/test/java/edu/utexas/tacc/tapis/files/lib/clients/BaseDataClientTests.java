package edu.utexas.tacc.tapis.files.lib.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.test.RandomByteInputStream;
import edu.utexas.tacc.tapis.files.test.RandomByteInputStream.SizeUnit;
import edu.utexas.tacc.tapis.files.test.TestUtils;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract public class BaseDataClientTests<T extends IRemoteDataClient> {
    private static Logger log = LoggerFactory.getLogger(BaseDataClientTests.class);
    private Map<String, TapisSystem> testSystems;
    private final String configPath;
    private final String testTenant = "dev";
    private final String testUser = "testuser";
    private final Path testRootPath = Path.of(UUID.randomUUID().toString());
    private final String configSection;

    protected BaseDataClientTests(String configPath) {
        this.configPath = configPath;
        this.configSection = this.getConfigSection();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        SshSessionPool.init();
        T dataClient = configureTestClient(testTenant, testUser, configSection);
        String testRootPathString = testRootPath.toString();
        try {
            dataClient.mkdir(testRootPathString);
        } catch (NotImplementedException ex) {
            logNotImplemented(ex);
        }
    }

    @AfterMethod
    public void afterMethod() throws Exception {
        T dataClient = configureTestClient(testTenant, testUser, configSection);
        String testRootPathString = testRootPath.toString();
        try {
            dataClient.delete(testRootPathString);
        } catch(Exception ex) {
            // ignore failed delete - it amy not exist
        }
    }


    @Test
    public void testMkDir() throws Exception {
        try {
            Path dirNamePath = Path.of(UUID.randomUUID().toString());
            Path pathForMkDir = testRootPath.resolve(dirNamePath);

            T dataClient = configureTestClient(testTenant, testUser, configSection);
            dataClient.mkdir(pathForMkDir.toString());
            List<FileInfo> fileInfos = dataClient.ls(testRootPath.toString());
            fileInfos = fileInfos.stream().filter(fileInfo -> {
                return Objects.equals(pathForMkDir, Path.of(fileInfo.getPath()));
            }).toList();

            Assert.assertEquals(fileInfos.size(), 1);
            FileInfo fileInfo = fileInfos.get(0);
            Assert.assertTrue(fileInfo.isDir());
            Assert.assertEquals(Path.of(fileInfo.getPath()), pathForMkDir);
            Assert.assertEquals(Path.of(fileInfo.getName()), dirNamePath.getFileName());
        } catch (NotImplementedException ex) {
            logNotImplemented(ex);
        }
    }

    @Test
    public void testMkDir_nested() throws Exception {
        try {
            String dir1 = UUID.randomUUID().toString();
            String dir2 = UUID.randomUUID().toString();
            String dir3 = UUID.randomUUID().toString();
            Path dirNamePath = Path.of(dir1, dir2, dir3);
            Path pathForMkDir = Path.of(testRootPath.toString(), dirNamePath.toString());

            T dataClient = configureTestClient(testTenant, testUser, configSection);
            dataClient.mkdir(pathForMkDir.toString());
            List<FileInfo> fileInfos = lsRecursive(dataClient, testRootPath.toString(), 5);
            fileInfos = fileInfos.stream().filter(fileInfo -> {
                return Objects.equals(pathForMkDir, Path.of(fileInfo.getPath()));
            }).toList();

            Assert.assertEquals(fileInfos.size(), 1);
            FileInfo fileInfo = fileInfos.get(0);
            Assert.assertTrue(fileInfo.isDir());
            Assert.assertEquals(Path.of(fileInfo.getPath()), pathForMkDir);
            Assert.assertEquals(Path.of(fileInfo.getName()), dirNamePath.getFileName());
        } catch (NotImplementedException ex) {
            logNotImplemented(ex);
        }
    }

    @Test
    public void testUploadAndDownload() throws Exception {
        long bytesToWrite = 1000;
        boolean alphaNumericOnly = true;
        String fileName = UUID.randomUUID().toString();
        String pathString = testRootPath.resolve(fileName).toString();

        T dataClient = configureTestClient(testTenant, testUser, configSection);
        RandomByteInputStream inputStream = new RandomByteInputStream(1500, SizeUnit.BYTES, true);
        dataClient.upload(testRootPath.resolve(fileName).toString(), inputStream);
        inputStream.close();
        String uploadHash = inputStream.getDigestString();

        InputStream downloadStream = dataClient.getStream(pathString);
        String downloadHash = TestUtils.getDigest(downloadStream);
        Assert.assertEquals(downloadHash, uploadHash);
    }

    abstract protected String getConfigSection();

    public List<FileInfo> lsRecursive(IRemoteDataClient dataClient, String pathString, int maxRecursion)
            throws Exception {
        List<FileInfo> fileInfos = dataClient.ls(pathString);
        List<FileInfo> childInfos = new ArrayList<FileInfo>();
        fileInfos.stream().forEach(fileInfo -> {
            if (fileInfo.isDir()) {
                try {
                    childInfos.addAll(lsRecursive(dataClient, fileInfo.getPath(), maxRecursion - 1));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        fileInfos.addAll(childInfos);
        return fileInfos;
    }

    protected T configureTestClient(String tenantName, String userName, String systemName) throws Exception {
        return configureTestClient(tenantName, userName, systemName, null, null);
    }

    protected T configureTestClient(String tenantName, String userName, String systemName,
                                 String impersonationId, String sharedCtxGrantor) throws Exception {
        TapisSystem system = readSystem(systemName);
        SystemsCache systemsCache = mock(SystemsCache.class);
        when(systemsCache.getSystem(tenantName, system.getId(), userName, impersonationId, sharedCtxGrantor)).thenReturn(system);
        T dataClient = createDataClient(tenantName, userName, system, systemsCache, impersonationId, sharedCtxGrantor);
        return dataClient;
    }

    protected Path relativizePath(Path path) {
        return testRootPath.relativize(path);
    }

    protected String relativizePath(String path) {
        return testRootPath.relativize(Path.of(path)).toString();
    }


    abstract public T createDataClient(String tenantName, String userName, TapisSystem system, SystemsCache systemsCache,
                                       String impersonationId, String sharedCtxGrantor) throws Exception;


    protected TapisSystem readSystem(String systemName) throws IOException {
        Map<String, TapisSystem> systemMap = readSystems();
        return systemMap.get(systemName);
    }

    protected Map<String, TapisSystem> readSystems() throws IOException {
        if (testSystems == null) {
            testSystems = TestUtils.readSystems(configPath);
        }
        return testSystems;
    }

    protected void logNotImplemented(NotImplementedException ex) {
        log.warn("Method not implemented:  " + ex.getMessage());
    }
}

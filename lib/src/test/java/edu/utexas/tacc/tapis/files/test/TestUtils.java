package edu.utexas.tacc.tapis.files.test;

import com.fasterxml.jackson.core.type.TypeReference;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.shared.utils.TapisObjectMapper;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.IOUtils;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class TestUtils {
    private static int CHUNK_MAX = 10240;
    private static final String SECRET_FILE_PREFIX = "secret-file:";

    public static TapisSystem readSystem(String configPath, String systemName) throws IOException {
        Map<String, TapisSystem> systemMap = readSystems(configPath);
        return systemMap.get(systemName);
    }

    public static Map<String, TapisSystem> readSystems(String configPath) throws IOException {
        Map<String, TapisSystem> systemMap = TapisObjectMapper.getMapper().readValue(TestUtils.class.getClassLoader().getResourceAsStream(configPath),
                    new TypeReference<Map<String, TapisSystem>>() {
                    });
        for(String key : systemMap.keySet()) {
            TapisSystem system = systemMap.get(key);
            Credential credential = system.getAuthnCredential();
            credential.setLoginUser(replaceSecret(credential, credential.getLoginUser()));
            credential.setPassword(replaceSecret(credential, credential.getPassword()));

            credential.setPublicKey(replaceSecret(credential, credential.getPublicKey()));
            credential.setPrivateKey(replaceSecret(credential, credential.getPrivateKey()));

            credential.setAccessKey(replaceSecret(credential, credential.getAccessKey()));
            credential.setAccessSecret(replaceSecret(credential, credential.getAccessSecret()));

            credential.setAccessToken(replaceSecret(credential, credential.getAccessToken()));
            credential.setCertificate(replaceSecret(credential, credential.getCertificate()));
            credential.setRefreshToken(replaceSecret(credential, credential.getRefreshToken()));
        }

        return systemMap;
    }

    private static String replaceSecret(Credential credential, String secret) throws IOException {
        String expandedSecret = secret;

        if((secret != null) && (secret.startsWith(SECRET_FILE_PREFIX))) {
            String filename = secret.substring(SECRET_FILE_PREFIX.length());
            expandedSecret = IOUtils.toString(TestUtils.class.getResourceAsStream(filename), StandardCharsets.UTF_8);
        }

        return expandedSecret;
    }

    public static String hashAsHex(byte[] hashBytes) {
        StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
        for (int i = 0; i < hashBytes.length; i++) {
            String hex = Integer.toHexString(0xff & hashBytes[i]);
            if(hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return "sha256:" + hexString.toString();
    }

    public static String getDigest(byte[] bytes) throws NoSuchAlgorithmException, IOException {
        return getDigest(new ByteArrayInputStream(bytes));
    }

    public static String getDigest(InputStream inputStream) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");


        long totalBytesRead = 0;
        int bytesRead = 0;
        byte[] chunk = new byte[CHUNK_MAX];
        do {
            bytesRead = inputStream.read(chunk);
            if(bytesRead > 0) {
                totalBytesRead += bytesRead;
                digest.update(chunk, 0, bytesRead);
            }
        } while (bytesRead > 0);

        return hashAsHex(digest.digest());
    }

    public static FilePermsService permsMock_AllowReadForSystem(String tenant, String user, String systemId) throws ServiceException {
        FilePermsService permsService = Mockito.mock(FilePermsService.class);

        // any path, read permission
        when(permsService.isPermitted(eq(tenant), eq(user), eq(systemId), any(), eq(FileInfo.Permission.READ))).thenReturn(true);
        return permsService;
    }

    public static FilePermsService permsMock_AllowModifyForSystem(String tenant, String user, String systemId) throws ServiceException {
        FilePermsService permsService = Mockito.mock(FilePermsService.class);

        // any path, modify permission
        when(permsService.isPermitted(eq(tenant), eq(user), eq(systemId), any(), eq(FileInfo.Permission.MODIFY))).thenReturn(true);
        return permsService;
    }

    public static FilePermsService permsMock_AllowModifyWithNoReadForSystem(String tenant, String user, String systemId) throws ServiceException {
        FilePermsService permsService = Mockito.mock(FilePermsService.class);

        // any path, modify permission, but no read
        when(permsService.isPermitted(eq(tenant), eq(user), eq(systemId), any(), eq(FileInfo.Permission.MODIFY))).thenReturn(true);
        when(permsService.isPermitted(eq(tenant), eq(user), eq(systemId), any(), eq(FileInfo.Permission.READ))).thenReturn(false);
        return permsService;
    }

    public static SystemsCache systemCacheMock_GetSystem(String tenant, String user, TapisSystem tapisSystem) throws ServiceException {
        SystemsCache sytemsCache = Mockito.mock(SystemsCache.class);

        // null impersonationId and sharedCtxGrantor
        when(sytemsCache.getSystem(tenant, tapisSystem.getId(), user, null, null)).thenReturn(tapisSystem);
        when(sytemsCache.getSystem(tenant, tapisSystem.getId(), user)).thenReturn(tapisSystem);
        return sytemsCache;
    }

    public static SystemsCacheNoAuth systemCacheNoAuthMock_GetSystem(String tenant, String user, TapisSystem tapisSystem) throws ServiceException {
        SystemsCacheNoAuth sytemsCacheNoAuth = Mockito.mock(SystemsCacheNoAuth.class);

        // null impersonationId and sharedCtxGrantor
        when(sytemsCacheNoAuth.getSystem(tenant, tapisSystem.getId(), user)).thenReturn(tapisSystem);
        return sytemsCacheNoAuth;
    }

}

package edu.utexas.tacc.tapis.files.lib.clients;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class DataClientUtils {

    public static String getRemotePath(@NotNull String rootDir, @NotNull String path) {
        String remotePath =  FilenameUtils.normalize(FilenameUtils.concat(rootDir, path));
        if (StringUtils.isEmpty(remotePath)) return "/";
        return remotePath;
    }

    public static String getRemotePathForS3(@NotNull String rootDir, @NotNull String path) {
        String remotePath = getRemotePath(rootDir, path);
        if ((StringUtils.isEmpty(remotePath)) || remotePath.equals("/")) return "/";
        return StringUtils.stripStart(remotePath, "/");
    }

    public static String ensureTrailingSlash(@NotNull String path) {
        return path.endsWith("/") ? path : path + "/";
    }

    public static boolean isDir(@NotNull String path) {
        return path.endsWith("/");
    }
}

package edu.utexas.tacc.tapis.files.lib.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtils {

    public static final Logger log = LoggerFactory.getLogger(PathUtils.class);

    /**
     * All paths are assumed to be relative to rootDir
     * @param srcBase The BASE path of the source of the transfer
     * @param srcPath The path to the actual file being transferred
     * @param destBase The BASE path of the destination of the transfer
     * @return Path
     */
    public static Path relativizePaths(String srcBase, String srcPath, String destBase) {

        // Make them look like absolute paths either way
        srcPath = StringUtils.prependIfMissing(srcPath, "/");
        srcBase = StringUtils.prependIfMissing(srcBase, "/");
        destBase = StringUtils.prependIfMissing(destBase, "/");

        Path sourcePath = Paths.get(srcPath);
        Path sourceBase = Paths.get(srcBase);


        // This happens if the source path is absolute, i.e. the transfer is for
        // a single file like a/b/c/file.txt
        if (sourceBase.equals(sourcePath) && destBase.endsWith("/")) {
            Path p = Paths.get(destBase, sourcePath.getFileName().toString());
            return p;
        } else {
            Path p = Paths.get(destBase, sourceBase.relativize(sourcePath).toString());
            return p;
        }

    }

}

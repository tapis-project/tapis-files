package edu.utexas.tacc.tapis.files.lib.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtils {

    public static final Logger log = LoggerFactory.getLogger(PathUtils.class);

    /**
     *
     * @param srcBase The BASE path of the source of the transfer
     * @param srcPath The path to the actual file being transferred
     * @param destBase The BASE path of the destination of the transfer
     * @return Path
     */
    public static Path relativizePaths(String srcBase, String srcPath, String destBase) {
        srcPath = StringUtils.prependIfMissing(srcPath, "/");
        srcBase = StringUtils.prependIfMissing(srcBase, "/");
        destBase = StringUtils.prependIfMissing(destBase, "/");

        Path sourcePath = Paths.get(srcPath);
        Path destinationBasePath = Paths.get(destBase);
        Path sourceBase = Paths.get(srcBase);

        Path tmp = destinationBasePath.resolve(sourceBase.relativize(sourcePath));

        return tmp;


    }

}

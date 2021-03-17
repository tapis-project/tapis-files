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
    public static Path relativizePathsForTransfer(String srcBase, String srcPath, String destBase) {
        //Make the paths absolute if they are not already.
        Path srcBasePath = Paths.get(StringUtils.prependIfMissing(srcBase, "/"));
        Path destBasePath = Paths.get(StringUtils.prependIfMissing(destBase, "/"));
        Path srcFilePath = Paths.get(StringUtils.prependIfMissing(srcPath, "/"));

        // This catches when the listing for the file is in the root of the tree, for instance
        // if srcPath = /jobs/input/file1.txt and destPath = /testFile.txt,
        //
        Path finalDestPath;
        if ( srcFilePath.equals(srcBasePath) ) {
            finalDestPath = destBasePath.normalize();
        } else {
            Path sourceRelativeToBase = srcBasePath.relativize(srcFilePath);
            finalDestPath = destBasePath.resolve(sourceRelativeToBase).normalize();
        }

        Path tmp = Paths.get(StringUtils.stripStart(finalDestPath.toString(), "/"));
        return tmp;
    }

}

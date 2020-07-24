package edu.utexas.tacc.tapis.files.lib.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtils {

    public static final Logger log = LoggerFactory.getLogger(PathUtils.class);

    public static Path relativizePathsForTransfer(String srcBase, String srcPath, String destBase) {
        //Make the paths absolute if they are not already.
        Path srcBasePath = Paths.get(StringUtils.prependIfMissing(srcBase, "/"));
        Path destBasePath = Paths.get(StringUtils.prependIfMissing(destBase, "/"));
        Path srcFilePath = Paths.get(StringUtils.prependIfMissing(srcPath, "/"));

        // This catches when the listing for the file is in the root of the tree.
        if ( srcFilePath.equals(srcBasePath) ) {
            return destBasePath.resolve(srcFilePath);
        }
        Path sourceRelativeToBase = srcBasePath.relativize(srcFilePath);
        Path finalDestPath = destBasePath.resolve(sourceRelativeToBase);
        return finalDestPath;
    }

}

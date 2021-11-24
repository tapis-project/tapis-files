package edu.utexas.tacc.tapis.files.lib.utils;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/*
 *
 * Utility class containing general use static methods for handling paths.
 * This class is non-instantiable
 *
 * Note that normalizing a path means:
 *  - double and single dot path steps are removed
 *  - multiple slashes are merged into a single slash
 *  - a trailing slash will be retained
 *  - if provided path is null or empty or resolving double dot steps results in no parent path
 *    then the relativePath becomes a single / resulting in the relativePath being the same as the system's rootDir.
 */
public class PathUtils
{
  // Private constructor to make it non-instantiable
  private PathUtils() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  public static final Logger log = LoggerFactory.getLogger(PathUtils.class);

  /**
   * Construct a normalized path intended to be relative to a system's rootDir based on a path provided by a user.
   * The apache commons method FilenameUtils.normalize() is expected to protect against escaping via ../..
   * In this case normalized means:
   *   - double and single dot path steps are removed
   *   - multiple slashes are merged into a single slash
   *   - a trailing slash will be removed
   *   - if provided path is null or empty or resolving double dot steps results in no parent path
   *       then the relativePath becomes the empty string resulting in the relativePath being the same as
   *       the system's rootDir.
   * @param path path provided by user
   * @return Path - normalized path
   */
  public static Path getRelativePath(String path)
  {
    Path emptyRelativePath = Paths.get("");
    if (StringUtils.isBlank(path) || "/".equals(path)) return emptyRelativePath;
    String relativePathStr = FilenameUtils.normalizeNoEndSeparator(path);
    if (StringUtils.isBlank(relativePathStr) || "/".equals(relativePathStr)) return emptyRelativePath;
    return Paths.get(relativePathStr);
  }

  /**
   * Construct a normalized absolute path given a system's rootDir and path relative to rootDir.
   * @param path path relative to system's rootDir
   * @return Path - normalized absolute path
   */
  public static Path getAbsolutePath(String rootDir, String path)
  {
    // If rootDir is null or empty use "/"
    String rdir = StringUtils.isBlank(rootDir) ? "/" : rootDir;
    // First get normalized relative path
    Path relativePath = getRelativePath(path);
    // Return constructed absolute path
    return Paths.get(rdir, relativePath.toString());
  }

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

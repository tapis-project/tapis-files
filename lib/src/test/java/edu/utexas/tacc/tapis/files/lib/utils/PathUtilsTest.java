package edu.utexas.tacc.tapis.files.lib.utils;

import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

@Test(groups = "integration")
public class PathUtilsTest {

  @Test
  public void testRelativePaths()
  {
    Path emptyPath = Paths.get("");
    Path p = PathUtils.getRelativePath(null);
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("/");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath(".");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("./");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("./.");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("/.");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("//");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("./..");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("..");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("../");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("/..");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("../..");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("./././..//.///.////./////.//////.");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("a/././..//.///.////./////.//////.");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("./a/./..//.///.////./////.//////.");
    Assert.assertEquals(p, emptyPath);
    p = PathUtils.getRelativePath("./a/b/./..//.///.////./////.//////.");
    Assert.assertEquals(p, Paths.get("a"));
    p = PathUtils.getRelativePath("a/b/c/./..//.///.////./////.//////.");
    Assert.assertEquals(p, Paths.get("a/b"));
    p = PathUtils.getRelativePath("a/b/c/././/.///.////./////.//////.");
    Assert.assertEquals(p, Paths.get("a/b/c"));
  }

  @Test
  public void testAbsolutePaths()
  {
    String rootDir = "/";
    Path rootPath = Paths.get(rootDir);
    Path p = PathUtils.getAbsolutePath(rootDir, null);
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "/");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, ".");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "./");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "./.");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "/.");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "//");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "./..");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "..");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "../");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "/..");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "../..");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "./././..//.///.////./////.//////.");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "a/././..//.///.////./////.//////.");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "./a/./..//.///.////./////.//////.");
    Assert.assertEquals(p, rootPath);
    p = PathUtils.getAbsolutePath(rootDir, "./a/b/./..//.///.////./////.//////.");
    Assert.assertEquals(p, Paths.get("/a"));
    p = PathUtils.getAbsolutePath(rootDir, "a/b/c/./..//.///.////./////.//////.");
    Assert.assertEquals(p, Paths.get("/a/b"));
    p = PathUtils.getAbsolutePath(rootDir, "a/b/c/././/.///.////./////.//////.");
    Assert.assertEquals(p, Paths.get("/a/b/c"));
    rootDir = "/a";
    p = PathUtils.getAbsolutePath(rootDir, "b");
    Assert.assertEquals(p, Paths.get("/a/b"));
    p = PathUtils.getAbsolutePath(rootDir, "b/c");
    Assert.assertEquals(p, Paths.get("/a/b/c"));
    p = PathUtils.getAbsolutePath(rootDir, "b/.//d/../c");
    Assert.assertEquals(p, Paths.get("/a/b/c"));
    p = PathUtils.getAbsolutePath(rootDir, "b/.//d/e/../..////////c");
    Assert.assertEquals(p, Paths.get("/a/b/c"));
  }

  @Test
    public void testSingleFileAbsolute() {
        Path p = PathUtils.relativizePaths("jobs/input/file1.txt",
            "jobs/input/file1.txt", "workdir/jobs/07416ca6-4b05-441f-b541-d9156355f1bd-007/file1.txt");
        Assert.assertEquals(p, Paths.get("/workdir/jobs/07416ca6-4b05-441f-b541-d9156355f1bd-007/file1.txt"));
    }

    @Test
    public void testSingleFileAbsoluteToRoot() {
        Path p = PathUtils.relativizePaths("jobs/input/",
            "jobs/input/file1.txt", "workdir/jobs/07416ca6-4b05-441f-b541-d9156355f1bd-007/");
        Assert.assertEquals(p, Paths.get("/workdir/jobs/07416ca6-4b05-441f-b541-d9156355f1bd-007/file1.txt"));
    }

    @Test
    public void testSingleFile1() {
        Path p = PathUtils.relativizePaths("jobs/input/file1.txt",
            "jobs/input/file1.txt", "workdir/jobs/07416ca6-4b05-441f-b541-d9156355f1bd-007/");
        Assert.assertEquals(p, Paths.get("/workdir/jobs/07416ca6-4b05-441f-b541-d9156355f1bd-007/file1.txt"));
    }

    @Test
    public void testSingleFile2() {
        Path p = PathUtils.relativizePaths("jobs/input/file1.txt",
            "jobs/input/file1.txt", "workdir/jobs/07416ca6-4b05-441f-b541-d9156355f1bd-007");
        Assert.assertEquals(p, Paths.get("/workdir/jobs/07416ca6-4b05-441f-b541-d9156355f1bd-007"));
    }

    @Test
    public void testRootOfDir() {
        Path p = PathUtils.relativizePaths("/", "/test.txt", "/");
        Assert.assertEquals(p, Paths.get("/test.txt"));
    }

    @Test
    public void testSimple() {
        //Transfer everything from folder /a on source to / on dest
        Path p = PathUtils.relativizePaths("/a", "/a/1.txt", "/");
        Assert.assertEquals(p, Paths.get("/1.txt"));
    }

    @Test
    public void testNotRoot() {
        //Transfer everything from folder /a on source to /b on dest
        Path p = PathUtils.relativizePaths("/a", "/a/1.txt", "/b");
        Assert.assertEquals(p, Paths.get("/b/1.txt"));
    }

    @Test
    public void testNested() {
        //Transfer everything from folder /a on source to /b on dest
        Path p = PathUtils.relativizePaths("/a", "/a/b/c/1.txt", "/b");
        Assert.assertEquals(p, Paths.get("/b/b/c/1.txt"));
    }

    @Test
    public void testNestedDest() {
        //Transfer everything from folder /a on source to /b/c/d/ on dest
        Path p = PathUtils.relativizePaths("/a", "/a/b/c/1.txt", "/b/c/d/");
        Assert.assertEquals(p, Paths.get("/b/c/d/b/c/1.txt"));
    }

    @Test
    public void testNestedSource() {
        //Transfer everything from folder /a/b/c/ on source to /on dest
        Path p = PathUtils.relativizePaths("/a/b/c/", "/a/b/c/1.txt", "/");
        Assert.assertEquals(p, Paths.get("/1.txt"));
    }

    @Test
    public void testFileAtRoot() {
        Path p = PathUtils.relativizePaths("sample1.txt", "sample1.txt", "test/");
        Assert.assertEquals(p, Paths.get("test/sample1.txt"));
    }

    @Test
    public void testFileInFolder() {
        Path p = PathUtils.relativizePaths("a/sample1.txt", "a/sample1.txt", "test/");
        Assert.assertEquals(p, Paths.get("test/sample1.txt"));
    }



}
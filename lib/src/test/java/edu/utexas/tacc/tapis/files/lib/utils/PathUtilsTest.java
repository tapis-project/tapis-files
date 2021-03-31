package edu.utexas.tacc.tapis.files.lib.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.testng.Assert.*;

@Test
public class PathUtilsTest {

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
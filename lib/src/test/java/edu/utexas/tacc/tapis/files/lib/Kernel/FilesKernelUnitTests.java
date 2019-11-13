package edu.utexas.tacc.tapis.files.lib.Kernel;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;
import edu.utexas.tacc.tapis.files.lib.kernel.SftpFilesKernel;
import edu.utexas.tacc.tapis.files.lib.kernel.UserInfoImplementation;

@Test(groups = {"unit"})
public class FilesKernelUnitTests {

    @Test
    public void transferFileSourceNotNull() throws FilesKernelException {
        final String src = null;
        final String dest = "path/to/destination";
        SftpFilesKernel kernelobj = new SftpFilesKernel("localhost", 22, "testuser", "password");
        boolean actual = kernelobj.transferFile(src, dest);
        boolean expected = false;
        Assert.assertEquals(actual, expected);

    }
    @Test
    public void transferFileDestNotNull() throws FilesKernelException {
        final String src = "path/to/source";
        final String dest = null;
        SftpFilesKernel kernelobj = new SftpFilesKernel("localhost", 22, "testuser", "password");
        boolean actual = kernelobj.transferFile(src, dest);
        boolean expected = false;
        Assert.assertEquals(actual, expected);

    }
    @Test
    public void isMFAPromptTestPositive()  {
        final String prompt = "TACC Token Code:";
        UserInfoImplementation uiobj = new UserInfoImplementation("testuser", "password");
        boolean actual = uiobj.isMFAPrompt(prompt);
        boolean expected = true;
        Assert.assertEquals(actual, expected);

    }
    @Test
    public void isMFAPromptTestNegative()  {
        final String prompt = ":";
        UserInfoImplementation uiobj = new UserInfoImplementation("testuser", "password");
        boolean actual = uiobj.isMFAPrompt(prompt);
        boolean expected = false;
        Assert.assertEquals(actual, expected);

    }


}

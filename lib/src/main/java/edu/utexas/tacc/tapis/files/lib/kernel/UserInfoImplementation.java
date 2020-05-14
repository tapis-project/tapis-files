package edu.utexas.tacc.tapis.files.lib.kernel;

import java.net.Socket;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;

/**
 * @author ajamthe
 * This class implements the abstract classes UserInfo and UIKeyboardInteractive
 * Received MFA prompt is compared with the known prompts error is reported as 
 * file transfer cannot be completed
 *
 *
 */
public class UserInfoImplementation implements UserInfo, UIKeyboardInteractive {

    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */

    private static final String[] KNOWN_MFA_PROMPTS = { "[sudo] password for: ", "TACC Token Code:",
            "select one of the following options", "Duo two-factor", "Yubikey for " };

    public String method;
    private String passwd;
    public String username;
    private byte[] privateKey;
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(UserInfoImplementation.class);


    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /**
     * Constructor
     * @param username
     * @param passwd
     */
    public UserInfoImplementation(String username, String passwd) {
        this.username = username;
        this.passwd = passwd;

    }

    public UserInfoImplementation(String username, byte[] privateKey) {
        this.username = username;
        this.privateKey = privateKey;

    }
    /* ********************************************************************** */
    /* Public methods */
    /* ********************************************************************** */


    @Override
    public String getPassphrase() {
        return null;
    }

    @Override
    public String getPassword() {
        return passwd;
    }

    @Override
    public boolean promptPassword(String message) {
        return true;
    }

    @Override
    public boolean promptPassphrase(String message) {
        return false;
    }

    @Override
    public boolean promptYesNo(String message) {
        return false;
    }

    @Override
    public void showMessage(String message) {
    }

    /**
     * This method catches the MFA prompt and logs it as error
     *
     */
    @Override
    public String[] promptKeyboardInteractive(String destination, String name, String instruction, String[] prompt,
                                              boolean[] echo) {
        String receivedPrompt = null;
        if (isMFAPrompt(prompt[0]))
            _log.debug("Known MFA prompt ");
        if (prompt.length != 0) {
            receivedPrompt = "Prompt received is " + prompt[0];
        }
        String Msg = "KERNEL_ERROR_RECEIVED_MFA_PROMPT "  + receivedPrompt;
        _log.error(Msg);
        return null;

    }

    /**
     * Checks the given string for the presence of any of a known set of MFA prompts
     * given in {@link #KNOWN_MFA_PROMPTS}.
     *
     * @param prompt the message returned from a kbi prompt
     * @return true if the prompt is a mfa challenge phrase, false otherwise
     */
    public boolean isMFAPrompt(String prompt) {
        for (String knownMFAPrompt : KNOWN_MFA_PROMPTS) {
            if (StringUtils.contains(prompt, knownMFAPrompt)) {
                return true;
            }
        }
        return false;
    }

}

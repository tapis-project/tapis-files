package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.dao.postits.PostItsDAO;

import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Used to clean up expired PostIts.  This includes PostIts that have been redeemed
 * the maximum count of times.
 */
public class PostItsReaper {

    private static final Logger log = LoggerFactory.getLogger(PostItsReaper.class);

    /**
     * Cleans up expired PostIts.
     *
     * @param postItsDAO The postItDAO.
     */
    public static void cleanup(PostItsDAO postItsDAO) {
        log.info(LibUtils.getMsg("POSTIT_REAPER_RUN"));
        try {
            int deleteCount = postItsDAO.deleteExpiredPostIts();
            log.info(LibUtils.getMsg("POSTIT_REAPER_COUNT", deleteCount));
        } catch (TapisException ex) {
            log.error(LibUtils.getMsg("POSTIT_REAPER_ERROR", ex.getMessage()), ex);
        }
    }
}

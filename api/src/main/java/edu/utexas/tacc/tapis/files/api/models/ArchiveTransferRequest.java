package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.uri.TapisUrl;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

import java.util.HashSet;
import java.util.Set;

public class ArchiveTransferRequest {
    private String sourceBaseUrl;
    private String destinationBaseUrl;
    private Set<String> relativePaths;
    private String srcSharedCtxGrantor;
    private String destSharedCtxGrantor;

    public String getSourceBaseUrl() {
        return sourceBaseUrl;
    }

    public void setSourceBaseUrl(String sourceBaseUrl) {
        this.sourceBaseUrl = sourceBaseUrl;
    }

    public String getDestinationBaseUrl() {
        return destinationBaseUrl;
    }

    public void setDestinationBaseUrl(String destinationBaseUrl) {
        this.destinationBaseUrl = destinationBaseUrl;
    }

    public void setRelativePaths(Set<String> relativePaths) {
        this.relativePaths = relativePaths;
    }

    public Set<String> getRelativePaths() {
        return relativePaths;
    }

    @Override
    public String toString() {
        return TapisUtils.toString(this);
    }

    public void setSrcSharedCtxGrantor(String srcSharedCtxGrantor) {
        this.srcSharedCtxGrantor = srcSharedCtxGrantor;
    }

    public String getSrcSharedCtxGrantor() {
        return srcSharedCtxGrantor;
    }

    public String getDestSharedCtxGrantor() {
        return destSharedCtxGrantor;
    }

    public void setDestSharedCtxGrantor(String destSharedCtxGrantor) {
        this.destSharedCtxGrantor = destSharedCtxGrantor;
    }

    public String validateRequest() {
        if(sourceBaseUrl == null) {
            return LibUtils.getMsg("FILES_XFER_NULL_PARAMETER", "ArchiveTransferRequest", "sourceBaseUrl");
        }

        if(destinationBaseUrl == null) {
            return LibUtils.getMsg("FILES_XFER_NULL_PARAMETER", "ArchiveTransferRequest", "destinationBaseUrl");
        }

        if((relativePaths == null) || (relativePaths.isEmpty())) {
            return LibUtils.getMsg("FILES_XFER_NULL_PARAMETER", "ArchiveTransferRequest", "relativePaths");
        }

        return null;
    }
}

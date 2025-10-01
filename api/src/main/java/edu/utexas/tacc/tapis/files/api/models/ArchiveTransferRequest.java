package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.transfers.ArchiveTransferProvider;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

import java.util.Set;

public class ArchiveTransferRequest {
    private String sourceBaseUrl;
    private String destinationBaseUrl;
    private Set<String> relativePaths;
    private String srcSharedCtxGrantor;
    private String destSharedCtxGrantor;
    private String archiveType = "TAR_GZIP";

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

    public Set<String> getRelativePaths() {
        return relativePaths;
    }

    public void setRelativePaths(Set<String> relativePaths) {
        this.relativePaths = relativePaths;
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

    public String getArchiveType() {
        return archiveType;
    }

    public void setArchiveType(String archiveType) {
        this.archiveType = archiveType;
    }

    public String validateRequest() {
        if(sourceBaseUrl == null) {
            return LibUtils.getMsg("FILES_XFER_NULL_PARAMETER", "ArchiveTransferRequest", "sourceBaseUrl");
        }

        if(destinationBaseUrl == null) {
            return LibUtils.getMsg("FILES_XFER_NULL_PARAMETER", "ArchiveTransferRequest", "destinationBaseUrl");
        }

        if((archiveType == null) || (archiveType.isEmpty())) {
            return LibUtils.getMsg("FILES_XFER_NULL_PARAMETER", "ArchiveTransferRequest", "archiveType");
        }

        try {
            // try to get the corresponding enum value - throws Illegal argument if it fails
            ArchiveTransferProvider.ArchiveType archiveTypeEnum = ArchiveTransferProvider.ArchiveType.valueOf(archiveType);

            switch(archiveTypeEnum) {
                case TAR, TAR_GZIP, TO_TAR_ARCHIVE, TO_GZIP_ARCHIVE -> {
                    if((relativePaths == null) || (relativePaths.isEmpty())) {
                        return LibUtils.getMsg("FILES_XFER_NULL_PARAMETER", "ArchiveTransferRequest", "relativePaths");
                    }
                }

                case FROM_TAR_ARCHIVE, FROM_GZIP_ARCHIVE -> {
                    if((relativePaths != null) && (!relativePaths.isEmpty())) {
                        return LibUtils.getMsg("FILES_XFER_INVALID_PARAMETER", "ArchiveTransferRequest", "relativePaths", "Must be null");
                    }
                }

            }
        } catch (IllegalArgumentException ex) {
            return LibUtils.getMsg("FILES_XFER_INVALID_PARAMETER", "ArchiveTransferRequest", "archiveType", ex.getMessage());
        }

        return null;
    }
}

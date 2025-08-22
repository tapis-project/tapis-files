package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferProvider;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jetbrains.annotations.UnmodifiableView;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ArchiveTransferParams {
    TransferURI srcUri = null;
    TransferURI dstUri = null;
    TapisSystem srcSystem = null;
    TapisSystem dstSystem = null;
    String srcSharedCtxGrantor = null;
    String dstSharedCtxGrantor = null;
    Set<String> relativePaths = new HashSet<>();
    ArchiveTransferProvider.ArchiveType archiveType;

    public TransferURI getSrcUri() {
        return srcUri;
    }

    public void setSrcUri(TransferURI srcUri) {
        this.srcUri = srcUri;
    }

    public TransferURI getDstUri() {
        return dstUri;
    }

    public void setDstUri(TransferURI dstUri) {
        this.dstUri = dstUri;
    }

    public TapisSystem getSrcSystem() {
        return srcSystem;
    }

    public void setSrcSystem(TapisSystem srcSystem) {
        this.srcSystem = srcSystem;
    }

    public TapisSystem getDstSystem() {
        return dstSystem;
    }

    public void setDstSystem(TapisSystem dstSystem) {
        this.dstSystem = dstSystem;
    }

    public String getSrcSharedCtxGrantor() {
        return srcSharedCtxGrantor;
    }

    public void setSrcSharedCtxGrantor(String srcSharedCtxGrantor) {
        this.srcSharedCtxGrantor = srcSharedCtxGrantor;
    }

    public String getDstSharedCtxGrantor() {
        return dstSharedCtxGrantor;
    }

    public void setDstSharedCtxGrantor(String dstSharedCtxGrantor) {
        this.dstSharedCtxGrantor = dstSharedCtxGrantor;
    }

    @UnmodifiableView
    public Set<String> getRelativePaths() {
        return Collections.unmodifiableSet(relativePaths);
    }

    public void setRelativePaths(Set<String> relativePaths) {
        this.relativePaths = relativePaths;
    }

    public ArchiveTransferProvider.ArchiveType getArchiveType() {
        return archiveType;
    }

    public void setArchiveType(ArchiveTransferProvider.ArchiveType archiveType) {
        this.archiveType = archiveType;
    }
}

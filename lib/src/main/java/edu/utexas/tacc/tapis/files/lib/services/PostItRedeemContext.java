package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;

import javax.ws.rs.core.StreamingOutput;

public class PostItRedeemContext {
    private String contentDisposition = null;
    private StreamingOutput outStream = null;
    private String mediaType = null;
    private boolean zip = false;

    ResourceRequestUser rUser;

    public StreamingOutput getOutStream() {
        return outStream;
    }

    public void setOutStream(StreamingOutput outStream) {
        this.outStream = outStream;
    }

    public String getContentDisposition() {
        return contentDisposition;
    }

    public void setContentDisposition(String contentDisposition) {
        this.contentDisposition = contentDisposition;
    }

    public String getMediaType() {
        return mediaType;
    }

    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    public boolean isZip() {
        return zip;
    }

    public void setZip(boolean zip) {
        this.zip = zip;
    }

    public ResourceRequestUser getrUser() {
        return rUser;
    }

    public void setrUser(ResourceRequestUser rUser) {
        this.rUser = rUser;
    }
}

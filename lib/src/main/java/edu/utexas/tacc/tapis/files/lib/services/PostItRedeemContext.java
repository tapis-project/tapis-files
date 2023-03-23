package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;

import javax.ws.rs.core.StreamingOutput;

public class PostItRedeemContext {
    private StreamingOutput outStream = null;
    private boolean zip = false;
    private String filename = null;

    ResourceRequestUser rUser;

    public StreamingOutput getOutStream() {
        return outStream;
    }

    public void setOutStream(StreamingOutput outStream) {
        this.outStream = outStream;
    }

    public boolean isZip() {
        return zip;
    }

    public void setZip(boolean zip) {
        this.zip = zip;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public ResourceRequestUser getrUser() {
        return rUser;
    }

    public void setrUser(ResourceRequestUser rUser) {
        this.rUser = rUser;
    }
}

package edu.utexas.tacc.tapis.files.lib.models;

import javax.validation.constraints.NotBlank;

public class TransferTaskRequestElement {

    private TransferURI sourceURI;
    private TransferURI destinationURI;
    private boolean optional;

    @NotBlank
    public TransferURI getSourceURI() {
        return sourceURI;
    }

    @NotBlank
    public TransferURI getDestinationURI() {
        return destinationURI;
    }

    public void setDestinationURI(TransferURI destinationURI) {
        this.destinationURI = destinationURI;
    }
    public void setDestinationURI(String destinationURI) {
        this.destinationURI = new TransferURI(destinationURI);
    }
    public void setSourceURI(TransferURI sourceURI) {
        this.sourceURI = sourceURI;
    }
    public void setSourceURI(String sourceURI) {
        this.sourceURI = new TransferURI(sourceURI);
    }

    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }
}

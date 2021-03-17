package edu.utexas.tacc.tapis.files.lib.models;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;

public class TransferTaskRequestElement {

    private TransferURI sourceURI;
    private TransferURI destinationURI;
    private boolean optional;

    @Schema(required = true, description = "Fully qualified URI, such as tapis://{systemID}/{path} or https://myserver.com/path/to/inputs/")
    @NotBlank
    public TransferURI getSourceURI() {
        return sourceURI;
    }

    @Schema(required = true, description = "Fully qualified URI to a tapis system, such as tapis://{systemID}/{path}")
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

    @Schema(description = "Allow the full transfer to succeed even if this portion fails? Default is false")
    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }
}

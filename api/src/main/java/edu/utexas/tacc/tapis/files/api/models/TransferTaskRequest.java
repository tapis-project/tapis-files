package edu.utexas.tacc.tapis.files.api.models;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;

public class TransferTaskRequest {

    private String sourceURI;
    private String destinationURI;


    @Schema(required = true, description = "Fully qualified URI, such as tapis://{systemID}/{path} or https://myserver.com/path/to/inputs/")
    @NotBlank
    public String getSourceURI() {
        return sourceURI;
    }

    @Schema(required = true, description = "Fully qualified URI to a tapis system, such as tapis://{systemID}/{path}")
    @NotBlank
    public String getDestinationURI() {
        return destinationURI;
    }

    public void setDestinationURI(String destinationURI) {
        this.destinationURI = destinationURI;
    }

    public void setSourceURI(String sourceURI) {
        this.sourceURI = sourceURI;
    }
}

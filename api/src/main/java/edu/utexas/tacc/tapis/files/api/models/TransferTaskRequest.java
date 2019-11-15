package edu.utexas.tacc.tapis.files.api.models;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.*;

public class TransferTaskRequest {

    private String sourceSystemId;
    private String sourcePath;
    private String destinationSystemId;
    private String destinationPath;

    @Schema(required = true, description = "ID of source system")
    @NotNull
    public String getSourceSystemId() {
        return sourceSystemId;
    }


    public void setSourceSystemId(String sourceSystemId) {
        this.sourceSystemId = sourceSystemId;
    }

    @Schema(required = true, description = "Path to file/folder in source system")
    @NotNull
    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    @Schema(required = true, description = "ID of destination system")
    @NotNull
    public String getDestinationSystemId() {
        return destinationSystemId;
    }

    public void setDestinationSystemId(String destinationSystemId) {
        this.destinationSystemId = destinationSystemId;
    }

    @Schema(required = true, description = "Path to file/folder in destination system")
    @NotNull
    public String getDestinationPath() {
        return destinationPath;
    }

    public void setDestinationPath(String destinationPath) {
        this.destinationPath = destinationPath;
    }
}

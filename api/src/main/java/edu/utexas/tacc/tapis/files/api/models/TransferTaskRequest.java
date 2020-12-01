package edu.utexas.tacc.tapis.files.api.models;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotEmpty;
import java.util.List;

public class TransferTaskRequest {

    private List<TransferTaskRequestElement> elements;

    @NotEmpty
    public List<TransferTaskRequestElement> getElements() {
        return elements;
    }

    public void setElements(List<TransferTaskRequestElement> elements) {
        this.elements = elements;
    }
}

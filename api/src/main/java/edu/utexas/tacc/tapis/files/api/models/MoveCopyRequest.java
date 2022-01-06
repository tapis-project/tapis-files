package edu.utexas.tacc.tapis.files.api.models;

import javax.validation.constraints.NotNull;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MoveCopyOperation;

public class MoveCopyRequest
{
  private MoveCopyOperation operation;
  private String newPath;

  @NotNull
  public MoveCopyOperation getOperation() { return operation; }

  public void setOperation(MoveCopyOperation o) {
    operation = o;
  }

  @NotNull
  public String getNewPath() {
    return newPath;
  }

  public void setNewPath(String s) {
    newPath = s;
  }
}

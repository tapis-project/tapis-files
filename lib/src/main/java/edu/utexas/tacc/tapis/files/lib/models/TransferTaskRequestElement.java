package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import javax.validation.constraints.NotBlank;

public class TransferTaskRequestElement
{
  public enum TransferType {
    TRANSFER,
    SERVICE_MOVE_DIRECTORY_CONTENTS,
    SERVICE_MOVE_FILE_OR_DIRECTORY
  };

  private TransferURI sourceURI;
  private TransferURI destinationURI;
  private boolean optional;
  private String srcSharedCtx;
  private String destSharedCtx;
  private  String tag;
  private TransferType transferType;

  @NotBlank
  public TransferURI getSourceURI() { return sourceURI; }

  @NotBlank
  public TransferURI getDestinationURI() { return destinationURI; }

  public void setDestinationURI(TransferURI t) { destinationURI = t; }
  public void setDestinationURI(String s) { destinationURI = new TransferURI(s); }
  public void setSourceURI(TransferURI t) { sourceURI = t; }
  public void setSourceURI(String s) { sourceURI = new TransferURI(s); }
  public boolean isOptional() { return optional; }
  public void setOptional(boolean b) { optional = b; }

  public String getSrcSharedCtx() { return srcSharedCtx; }
  public void setSrcSharedCtx(String s) { srcSharedCtx = s; }
  public String getDestSharedCtx() { return destSharedCtx; }
  public void setDestSharedCtx(String s) { destSharedCtx = s; }

  public String getTag() { return tag; }
  public void setTag(String s) { tag = s; }

  public TransferType getTransferType() {
    return transferType;
  }

  public void setTransferType(TransferType transferType) {
    this.transferType = transferType;
  }

  public String toString() {return TapisUtils.toString(this);}
}

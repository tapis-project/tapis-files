package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

import javax.validation.constraints.NotBlank;

public class TransferTaskRequestElement
{
  private TransferURI sourceURI;
  private TransferURI destinationURI;
  private boolean optional;
  private boolean srcSharedAppCtx = false;
  private boolean destSharedAppCtx = false;

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
  public boolean isSrcSharedAppCtx() { return srcSharedAppCtx; }
  public void setSrcSharedAppCtx(boolean b) { srcSharedAppCtx = b; }
  public boolean isDestSharedAppCtx() { return destSharedAppCtx; }
  public void setDestSharedAppCtx(boolean b) { destSharedAppCtx = b; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}

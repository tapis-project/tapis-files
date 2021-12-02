package edu.utexas.tacc.tapis.files.lib.models;

import javax.validation.constraints.NotBlank;

public class TransferTaskRequestElement
{
  private TransferURI sourceURI;
  private TransferURI destinationURI;
  private boolean optional;

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
}

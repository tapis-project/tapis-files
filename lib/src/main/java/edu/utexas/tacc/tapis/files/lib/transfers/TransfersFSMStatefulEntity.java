package edu.utexas.tacc.tapis.files.lib.transfers;

import org.statefulj.persistence.annotations.State;

public class TransfersFSMStatefulEntity  implements ITransfersFSMStatefulEntity
{
    @State
    String state;   // Memory Persister requires a String

    @Override
    public String getState(){return state;}
}

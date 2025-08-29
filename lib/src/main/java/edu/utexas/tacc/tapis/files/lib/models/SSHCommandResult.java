package edu.utexas.tacc.tapis.files.lib.models;

public class SSHCommandResult {
    private int commandResult;
    private byte[] commandError;
    private byte[] commandOutput;

    public int getCommandResult() {
        return commandResult;
    }

    public void setCommandResult(int commandResult) {
        this.commandResult = commandResult;
    }

    public byte[] getCommandError() {
        return commandError;
    }

    public void setCommandError(byte[] commandError) {
        this.commandError = commandError;
    }

    public byte[] getCommandOutput() {
        return commandOutput;
    }

    public void setCommandOutput(byte[] commandOutput) {
        this.commandOutput = commandOutput;
    }

    public boolean isSuccess() {
        return commandResult == 0;
    }
}

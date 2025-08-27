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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Result: ");
        sb.append(commandResult);
        sb.append(" Message: ");
        if(commandError == null) {
            sb.append("<null>");
        } else {
            String error = new String(commandError);
            sb.append(error);
        }
        sb.append(System.lineSeparator());
        sb.append("Output: ");
        sb.append((commandOutput == null) ? null : sb.append(new String(commandOutput)));
        sb.append(System.lineSeparator());
        return sb.toString();
    }
}

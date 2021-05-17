package edu.utexas.tacc.tapis.files.lib.models;

/*
 * Class containing results from running a Linux command
 */
public class NativeLinuxOpResult
{
  private String command;
  private int exitCode;
  private String stdOut;
  private String stdErr;

  public NativeLinuxOpResult() { }

  public NativeLinuxOpResult(String cmd1, int exitStatus1, String stdOut1, String stdErr1)
  {
    command = cmd1;
    exitCode = exitStatus1;
    stdOut = stdOut1;
    stdErr = stdErr1;
  }

  public String getCommand() {
        return command;
    }
  public int getExitCode() { return exitCode; }
  public String getStdOut() {
    return stdOut;
  }
  public String getStdErr() {
        return stdErr;
    }

  public String toString()
  {
    return String.format("Command: %s%n ExitCode: %d%n StdOut: %s%n StdErr: %s%n", command, exitCode, stdOut, stdErr);
  }
}
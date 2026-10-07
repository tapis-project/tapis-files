package edu.utexas.tacc.tapis.files.lib.utils;

/*
   Utility class containing general use static methods related to threads/concurrency.
   This class is non-instantiable
 */
public class TransferWorkerUtils
{
  // Private constructor to make it non-instantiable
  private TransferWorkerUtils() { throw new AssertionError(); }

  /**
   * Sleep for the given number of milliseconds, restoring the interrupt status of the
   * current thread if interrupted while sleeping. Intended for use as a backoff between
   * iterations of a polling loop.
   */
  public static void sleepBriefly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}

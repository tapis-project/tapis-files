package edu.utexas.tacc.tapis.files.lib.kernel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.SftpProgressMonitor;

public class ProgressMonitor implements SftpProgressMonitor {
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(ProgressMonitor.class.getName());

	private long max = 0;
	private long count = 0;
	private long percent = 0;

	public ProgressMonitor() {
	}

	/**
	 * Will be called when a new operation starts.
	 * op - a code indicating the direction of transfer, one of PUT and GET
	 * dest - the destination file name.
	 * max - the final count (i.e. length of file to transfer).
	 */
	@Override public void init(int op, String src, String dest, long max) {
		this.max = max;
		if (max < 0) 
			max = 0;
		_log.info("starting copy from source: " + src + " to destination " + dest + " total file size:  " + max);

	}

	/**
	 * Will be called periodically as more data is transfered.
	 * count - the number of bytes transferred so far
	 * true if the transfer should go on, false if the transfer should be cancelled.
	 */
	@Override public boolean count(long bytes) {
		this.count += bytes;
		long percentNow = this.count * 100 / max;
		if (percentNow > this.percent) {
			this.percent = percentNow;
			_log.info("progress " + this.percent); // Progress 0,0
			_log.info("Total file size: " + max + " bytes"); // total file size
			_log.info("Bytes copied: " + this.count);// Progress in bytes from the total
		}
		return true;
	}

	
	/**
	 * Will be called when the transfer ended, either because all the data was transferred, 
	 * or because the transfer was cancelled.
	 */
	@Override public void end() {
		_log.debug("finished copying " + this.percent + "%");

	}
}

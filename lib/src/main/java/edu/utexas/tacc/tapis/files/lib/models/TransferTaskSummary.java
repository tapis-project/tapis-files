package edu.utexas.tacc.tapis.files.lib.models;

public class TransferTaskSummary {

    private long estimatedTotalBytes;
    private long totalBytesTransferred;
    private int totalTransfers;
    private int completeTransfers;


    public long getEstimatedTotalBytes() {
        return estimatedTotalBytes;
    }

    public void setEstimatedTotalBytes(long estimatedTotalBytes) {
        this.estimatedTotalBytes = estimatedTotalBytes;
    }

    public long getTotalBytesTransferred() {
        return totalBytesTransferred;
    }

    public void setTotalBytesTransferred(long totalBytesTransferred) {
        this.totalBytesTransferred = totalBytesTransferred;
    }

    public int getTotalTransfers() {
        return totalTransfers;
    }

    public void setTotalTransfers(int totalTransfers) {
        this.totalTransfers = totalTransfers;
    }

    public int getCompleteTransfers() {
        return completeTransfers;
    }

    public void setCompleteTransfers(int completeTransfers) {
        this.completeTransfers = completeTransfers;
    }
}

package edu.utexas.tacc.tapis.files.lib.services;

public class FileListingOpts {

    public static class Builder {
        private FileListingOpts fileListingOpts;
        public Builder() {
            clear();
        }

        public void clear() {
            this.fileListingOpts = new FileListingOpts();
        }

        public Builder setPageSize(int pageSize) {
            this.fileListingOpts.setPageSize(pageSize);
            return this;
        }

        public Builder setItemOffset(long itemOffset) {
            this.fileListingOpts.setItemOffset(itemOffset);
            return this;
        }
        public Builder setRecursionLimit(int recursionLimit) {
            this.fileListingOpts.setRecursionLimit(recursionLimit);
            return this;
        }

        public Builder setPattern(String pattern) {
            this.fileListingOpts.setPattern(pattern);
            return this;
        }

        public Builder setRecurse(boolean recurse) {
            this.fileListingOpts.setRecurse(recurse);
            return this;
        }

        public FileListingOpts build() {
            return this.fileListingOpts;
        }
    }

    public static final int DEFAULT_PAGE_SIZE = Integer.MAX_VALUE;
    public static final int DEFAULT_ITEM_OFFSET = 0;
    public static final boolean DEFAULT_RECURSE = false;
    public static final int DEFAULT_RECURSION_LIMIT = 20;
    public static final String DEFAULT_PATTERN = null;

    private int pageSize;
    private long itemOffset;
    private int recursionLimit;
    private String pattern;
    private boolean recurse;

    private FileListingOpts() {
        this.pageSize = DEFAULT_PAGE_SIZE;
        this.itemOffset = DEFAULT_ITEM_OFFSET;
        this.recurse = DEFAULT_RECURSE;
        this.recursionLimit = DEFAULT_RECURSION_LIMIT;
        this.pattern = DEFAULT_PATTERN;
    }

    public int getPageSize() {
        return pageSize;
    }

    private void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public long getItemOffset() {
        return itemOffset;
    }

    private void setItemOffset(long itemOffset) {
        this.itemOffset = itemOffset;
    }

    public int getRecursionLimit() {
        return recursionLimit;
    }

    private void setRecursionLimit(int recursionLimit) {
        this.recursionLimit = recursionLimit;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public boolean isRecurse() {
        return recurse;
    }

    public void setRecurse(boolean recurse) {
        this.recurse = recurse;
    }
}


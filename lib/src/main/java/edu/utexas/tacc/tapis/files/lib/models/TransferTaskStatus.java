package edu.utexas.tacc.tapis.files.lib.models;

// TODO: move this to class TransferTask
// TODO/TBD: Add FAILED_OPT for child and parent tasks that fail but are optional?
public enum TransferTaskStatus { ACCEPTED, STAGED, IN_PROGRESS, COMPLETED, CANCELLED, FAILED, FAILED_OPT, PAUSED, UNKNOWN }

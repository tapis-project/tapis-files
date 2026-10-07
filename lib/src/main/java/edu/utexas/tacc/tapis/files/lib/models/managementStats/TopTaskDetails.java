package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import java.util.ArrayList;
import java.util.List;

public class TopTaskDetails {
    // top
    private List<TopTaskInfo> topInProgress = new ArrayList<>();
    private List<TopTaskInfo> recentErrors = new ArrayList<>();
    private List<TopTaskInfo> recentSuccesses = new ArrayList<>();

    public List<TopTaskInfo> getTopInProgress() {
        return topInProgress;
    }

    public List<TopTaskInfo> getRecentSuccesses() {
        return recentSuccesses;
    }

    public List<TopTaskInfo> getRecentErrors() {
        return recentErrors;
    }
}

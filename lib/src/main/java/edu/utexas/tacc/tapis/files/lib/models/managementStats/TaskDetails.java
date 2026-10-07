package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import java.util.ArrayList;
import java.util.List;

public class TaskDetails {
    private TopTaskInfo topTaskInfo = null;
    private List<ParentTaskInfo> parentTaskInfos = new ArrayList<>();
    private List<ChildTaskInfo> childTaskInfos = new ArrayList<>();

    public TopTaskInfo getTopTaskInfo() {
        return topTaskInfo;
    }

    public void setTopTaskInfo(TopTaskInfo topTaskInfo) {
        this.topTaskInfo = topTaskInfo;
    }

    public List<ParentTaskInfo> getParentTaskInfos() {
        return parentTaskInfos;
    }

    public List<ChildTaskInfo> getChildTaskInfos() {
        return childTaskInfos;
    }
}

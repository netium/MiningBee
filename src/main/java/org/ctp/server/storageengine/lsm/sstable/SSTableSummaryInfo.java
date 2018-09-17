package org.ctp.server.storageengine.lsm.sstable;

public class SSTableSummaryInfo {
    private long dataItems;

    public long getDataItems() {
        return dataItems;
    }

    public void setDataItems(long dataItems) {
        this.dataItems = dataItems;
    }
}

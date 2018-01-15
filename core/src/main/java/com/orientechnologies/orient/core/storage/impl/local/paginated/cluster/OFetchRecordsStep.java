package com.orientechnologies.orient.core.storage.impl.local.paginated.cluster;

import com.orientechnologies.orient.core.storage.ORawBuffer;

public class OFetchRecordsStep {
  private final long nextPageIndex;

  private final ORawBuffer[] records;
  private final long[]       clusterPositions;

  public OFetchRecordsStep(long nextPageIndex, ORawBuffer[] records, long[] clusterPositions) {
    this.nextPageIndex = nextPageIndex;
    this.records = records;
    this.clusterPositions = clusterPositions;
  }

  public long getNextPageIndex() {
    return nextPageIndex;
  }

  public ORawBuffer[] getRecords() {
    return records;
  }

  public long[] getClusterPositions() {
    return clusterPositions;
  }
}

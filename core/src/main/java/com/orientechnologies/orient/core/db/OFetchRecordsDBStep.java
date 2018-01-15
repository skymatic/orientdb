package com.orientechnologies.orient.core.db;

public class OFetchRecordsDBStep<T> {
  private final long      nextPageIndex;
  private final T[] records;

  public OFetchRecordsDBStep(long nextPageIndex, T[] records) {
    this.nextPageIndex = nextPageIndex;
    this.records = records;
  }

  public long getNextPageIndex() {
    return nextPageIndex;
  }

  public T[] getRecords() {
    return records;
  }
}

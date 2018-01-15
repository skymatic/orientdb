package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.record.ORecordOperation;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class OPaginatedClusterRecordsIterator<T> implements Iterator<T> {
  private final ODatabaseInternal<T> db;
  private final int                  clusterId;

  private T[]  records;
  private long nextPage;
  private int  nextRecord;

  private List<ORecordOperation> txEntries;
  private int                    txEntryIndex;

  public OPaginatedClusterRecordsIterator(ODatabaseInternal<T> db, int clusterId) {
    this.db = db;
    this.clusterId = clusterId;
    this.txEntries = db.getTransaction().getNewRecordEntriesByClusterIds(new int[] { clusterId });
    if (txEntries == null) {
      txEntries = Collections.emptyList();
    }
  }

  @Override
  public boolean hasNext() {
    fetchNextRecordIfNeeded();

    if (nextRecord < records.length) {
      return true;
    }

    if (txEntryIndex < txEntries.size()) {
      return true;
    }
    return false;
  }

  private void fetchNextRecordIfNeeded() {
    if (records == null) {
      final OFetchRecordsDBStep<T> step = db.fetchRecords(clusterId, -1);
      records = step.getRecords();
      nextPage = step.getNextPageIndex();
      nextRecord = 0;
    }

    while (nextRecord >= records.length && nextPage != -1) {
      final OFetchRecordsDBStep<T> step = db.fetchRecords(clusterId, nextPage);

      records = step.getRecords();
      nextPage = step.getNextPageIndex();
      nextRecord = 0;
    }

  }

  @Override
  public T next() {
    fetchNextRecordIfNeeded();

    if (nextRecord >= records.length && txEntryIndex >= txEntries.size()) {
      throw new NoSuchElementException();
    }

    final T record;

    if (nextRecord < records.length) {
      record = records[nextRecord];
      nextRecord++;
    } else {
      //noinspection unchecked
      record = (T) txEntries.get(txEntryIndex).getRecord();
      txEntryIndex++;
    }

    return record;
  }
}

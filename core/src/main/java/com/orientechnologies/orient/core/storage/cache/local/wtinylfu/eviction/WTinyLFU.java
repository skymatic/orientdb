package com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.PageKey;

import java.util.concurrent.ConcurrentHashMap;

public final class WTinyLFU {
  private static final int EDEN_PERCENT         = 20;
  private static final int PROBATIONARY_PERCENT = 20;

  private volatile int                                     size;
  private volatile int                                     maxSize;
  private final    ConcurrentHashMap<PageKey, OCacheEntry> data;
  private final    Admittor<PageKey>                       admittor;

  private int edenSize;
  private int probationarySize;
  private int protectedSize;

  public WTinyLFU(ConcurrentHashMap<PageKey, OCacheEntry> data, Admittor<PageKey> admittor) {
    this.data = data;
    this.admittor = admittor;
  }

  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
    calculateMaxSizes();

    admittor.ensureCapacity(maxSize);
  }

  public int getMaxSize() {
    return maxSize;
  }

  public int getSize() {
    return size;
  }

  public void onAccess(OCacheEntry cacheEntry) {
  }

  public void onAdd(OCacheEntry cacheEntry) {
  }

  public void onPinning(OCacheEntry cacheEntry) {
  }

  private void calculateMaxSizes() {
    edenSize = maxSize * EDEN_PERCENT / 100;
    probationarySize = (maxSize - edenSize) * PROBATIONARY_PERCENT / 100;
    protectedSize = maxSize - edenSize - probationarySize;
  }
}

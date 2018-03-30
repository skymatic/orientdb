package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction.LRUList;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesPortion;

/**
 * Created by tglman on 23/06/16.
 */
public class OCacheEntryChanges implements OCacheEntry {

  OCacheEntry delegate;
  final OWALChanges changes = new OWALPageChangesPortion();

  boolean            isNew   = false;
  boolean            pinPage = false;

  public OCacheEntryChanges(OCacheEntry entry) {
    delegate = entry;
  }

  @SuppressWarnings("WeakerAccess")
  public OCacheEntryChanges() {
  }

  @Override
  public void markDirty() {
    delegate.markDirty();
  }

  @Override
  public void clearDirty() {
    delegate.clearDirty();
  }

  @Override
  public boolean isDirty() {
    return delegate.isDirty();
  }

  @Override
  public OCachePointer getCachePointer() {
    return delegate.getCachePointer();
  }

  @Override
  public void clearCachePointer() {
    delegate.clearCachePointer();
  }

  @Override
  public void setCachePointer(OCachePointer cachePointer) {
    delegate.setCachePointer(cachePointer);
  }

  @Override
  public long getFileId() {
    return delegate.getFileId();
  }

  @Override
  public long getPageIndex() {
    return delegate.getPageIndex();
  }

  @Override
  public void acquireExclusiveLock() {
    delegate.acquireExclusiveLock();
  }

  @Override
  public void releaseExclusiveLock() {
    delegate.releaseExclusiveLock();
  }

  @Override
  public void acquireSharedLock() {
    delegate.acquireSharedLock();
  }

  @Override
  public void releaseSharedLock() {
    delegate.releaseSharedLock();
  }

  @Override
  public int getUsagesCount() {
    return delegate.getUsagesCount();
  }

  @Override
  public void incrementUsages() {
    delegate.incrementUsages();
  }

  @Override
  public boolean isLockAcquiredByCurrentThread() {
    return delegate.isLockAcquiredByCurrentThread();
  }

  @Override
  public void decrementUsages() {
    delegate.decrementUsages();
  }

  @Override
  public OWALChanges getChanges() {
    return changes;
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  @Override
  public boolean acquireEntry() {
    return delegate.acquireEntry();
  }

  @Override
  public void releaseEntry() {
    delegate.releaseEntry();
  }

  @Override
  public boolean isReleased() {
    return delegate.isReleased();
  }

  @Override
  public boolean makePinned() {
    return delegate.makePinned();
  }

  @Override
  public boolean isPinned() {
    return delegate.isPinned();
  }

  @Override
  public boolean isAlive() {
    return delegate.isAlive();
  }

  @Override
  public boolean freeze() {
    return delegate.freeze();
  }

  @Override
  public void makeDead() {
    delegate.makeDead();
  }

  @Override
  public boolean isDead() {
    return delegate.isDead();
  }

  @Override
  public OCacheEntry getNext() {
    return delegate.getNext();
  }

  @Override
  public OCacheEntry getPrev() {
    return delegate.getPrev();
  }

  @Override
  public void setPrev(OCacheEntry prev) {
    delegate.setPrev(prev);
  }

  @Override
  public void setNext(OCacheEntry next) {
    delegate.setNext(next);
  }

  @Override
  public void setContainer(LRUList lruList) {
    delegate.setContainer(lruList);
  }

  @Override
  public LRUList getContainer() {
    return delegate.getContainer();
  }

  public void setDelegate(OCacheEntry delegate) {
    this.delegate = delegate;
  }

  public OCacheEntry getDelegate() {
    return delegate;
  }
}

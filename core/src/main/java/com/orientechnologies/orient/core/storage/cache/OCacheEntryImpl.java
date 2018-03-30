package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction.LRUList;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tglman on 23/06/16.
 */
public class OCacheEntryImpl implements OCacheEntry {
  private static final int FREEZED = -1;
  private static final int DEAD    = -2;

  private       OCachePointer pointer;
  private final long          fileId;
  private final long          pageIndex;

  private boolean dirty;
  private final AtomicInteger usagesCount = new AtomicInteger();
  private final AtomicInteger state       = new AtomicInteger();
  private volatile boolean pinned;

  private OCacheEntry next;
  private OCacheEntry prev;

  private LRUList container;

  public OCacheEntryImpl(long fileId, long pageIndex, OCachePointer pointer, boolean dirty) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;

    this.pointer = pointer;
    this.dirty = dirty;
  }

  @Override
  public void markDirty() {
    this.dirty = true;
  }

  @Override
  public void clearDirty() {
    this.dirty = false;
  }

  @Override
  public boolean isDirty() {
    return dirty;
  }

  @Override
  public OCachePointer getCachePointer() {
    return pointer;
  }

  @Override
  public void clearCachePointer() {
    pointer = null;
  }

  @Override
  public void setCachePointer(OCachePointer cachePointer) {
    this.pointer = cachePointer;
  }

  @Override
  public long getFileId() {
    return fileId;
  }

  @Override
  public long getPageIndex() {
    return pageIndex;
  }

  @Override
  public void acquireExclusiveLock() {
    pointer.acquireExclusiveLock();
  }

  @Override
  public void releaseExclusiveLock() {
    pointer.releaseExclusiveLock();
  }

  @Override
  public void acquireSharedLock() {
    pointer.acquireSharedLock();
  }

  @Override
  public void releaseSharedLock() {
    pointer.releaseSharedLock();
  }

  @Override
  public int getUsagesCount() {
    return usagesCount.get();
  }

  @Override
  public void incrementUsages() {
    usagesCount.incrementAndGet();
  }

  /**
   * DEBUG only !!
   *
   * @return Whether lock acquired on current entry
   */
  @Override
  public boolean isLockAcquiredByCurrentThread() {
    return pointer.isLockAcquiredByCurrentThread();
  }

  @Override
  public void decrementUsages() {
    usagesCount.decrementAndGet();
  }

  @Override
  public OWALChanges getChanges() {
    return null;
  }

  @Override
  public int getSize() {
    return pointer.getSize();
  }

  @Override
  public boolean acquireEntry() {
    int state = this.state.get();

    while (state >= 0) {
      if (this.state.compareAndSet(state, state + 1)) {
        return true;
      }

      state = this.state.get();
    }

    return false;
  }

  @Override
  public void releaseEntry() {
    int state = this.state.get();

    while (true) {
      if (state <= 0) {
        throw new IllegalStateException("Cache entry " + fileId + ":" + pageIndex + " has invalid state " + state);
      }

      if (this.state.compareAndSet(state, state - 1)) {
        return;
      }

      state = this.state.get();
    }
  }

  @Override
  public boolean isReleased() {
    return state.get() == 0;
  }

  @Override
  public boolean makePinned() {
    if (state.get() > 0) {
      return false;
    }

    if (!pinned) {
      pinned = true;
    }

    return true;
  }

  @Override
  public boolean isPinned() {
    return pinned;
  }

  @Override
  public boolean isAlive() {
    return state.get() >= 0;
  }

  @Override
  public boolean freeze() {
    int state = this.state.get();
    while (state == 0) {
      if (this.state.compareAndSet(state, FREEZED)) {
        return true;
      }

      state = this.state.get();
    }

    return false;
  }

  @Override
  public void makeDead() {
    int state = this.state.get();

    while (state <= 0) {
      if (this.state.compareAndSet(state, DEAD)) {
        return;
      }

      state = this.state.get();
    }

    throw new IllegalStateException("Cache entry " + fileId + ":" + pageIndex + " has invalid state " + state);
  }

  @Override
  public boolean isDead() {
    return this.state.get() == DEAD;
  }

  @Override
  public OCacheEntry getNext() {
    return next;
  }

  @Override
  public OCacheEntry getPrev() {
    return prev;
  }

  @Override
  public void setPrev(OCacheEntry prev) {
    this.prev = prev;
  }

  @Override
  public void setNext(OCacheEntry next) {
    this.next = next;
  }

  @Override
  public void setContainer(LRUList lruList) {
    this.container = lruList;
  }

  @Override
  public LRUList getContainer() {
    return container;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OCacheEntryImpl that = (OCacheEntryImpl) o;

    if (fileId != that.fileId)
      return false;
    if (dirty != that.dirty)
      return false;
    if (pageIndex != that.pageIndex)
      return false;
    if (usagesCount.get() != that.usagesCount.get())
      return false;
    if (pointer != null ? !pointer.equals(that.pointer) : that.pointer != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (pointer != null ? pointer.hashCode() : 0);
    result = 31 * result + (dirty ? 1 : 0);
    result = 31 * result + usagesCount.get();
    return result;
  }

  @Override
  public String toString() {
    return "OReadCacheEntry{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + ", pointer=" + pointer + ", dirty=" + dirty
        + ", usagesCount=" + usagesCount + '}';
  }

}

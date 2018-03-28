package com.orientechnologies.orient.core.storage.cache.local.wtinylfu;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OReadCacheException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class WTinyLFUReadCache implements OReadCache {
  /**
   * Maximum amount of times when we will show message that limit of pinned pages was exhausted.
   */
  private static final int MAX_AMOUNT_OF_WARNINGS_PINNED_PAGES = 10;

  private static final int                                     _4K_PAGE    = 4 * 1024;
  private final        OReadersWriterSpinLock                  cacheLock   = new OReadersWriterSpinLock();
  private final        ConcurrentHashMap<PageKey, OCacheEntry> data        = new ConcurrentHashMap<>();
  private final        ConcurrentHashMap<PageKey, OCacheEntry> pinnedPages = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Long, OReadersWriterSpinLock> fileLocks = new ConcurrentHashMap<>();

  private final AtomicInteger pinnedPagesSize = new AtomicInteger();

  private volatile int cacheSize = 0;
  private final int percentOfPinnedPages;

  private final AtomicInteger maxSize = new AtomicInteger();

  /**
   * Counts how much time we warned user that limit of amount of pinned pages is reached.
   */
  private final AtomicInteger pinnedPagesWarningCounter = new AtomicInteger();

  public WTinyLFUReadCache() {
    percentOfPinnedPages = 1;
  }

  @Override
  public long addFile(String fileName, OWriteCache writeCache) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      return writeCache.addFile(fileName);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public long addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      return writeCache.addFile(fileName, fileId);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public OCacheEntry loadForWrite(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums) {
    cacheLock.acquireReadLock();
    try {
      OReadersWriterSpinLock fileLock = getFileLock(fileId);
      fileLock.acquireReadLock();
      try {
        final OCacheEntry cacheEntry = doLoad(fileId, (int) pageIndex, checkPinnedPages, writeCache, false, verifyChecksums);

        if (cacheEntry != null) {
          cacheEntry.acquireExclusiveLock();
          writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer());
        }

        return cacheEntry;
      } finally {
        fileLock.releaseReadLock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public OCacheEntry loadForRead(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums) {
    cacheLock.acquireReadLock();
    try {
      OReadersWriterSpinLock fileLock = getFileLock(fileId);
      fileLock.acquireReadLock();
      try {
        final OCacheEntry cacheEntry = doLoad(fileId, (int) pageIndex, checkPinnedPages, writeCache, false, verifyChecksums);

        if (cacheEntry != null) {
          cacheEntry.acquireSharedLock();
        }

        return cacheEntry;
      } finally {
        fileLock.releaseReadLock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  private OCacheEntry doLoad(long fileId, int pageIndex, boolean checkPinnedPages, OWriteCache writeCache, boolean addNewPages,
      boolean verifyChecksums) {

    final PageKey pageKey = new PageKey(fileId, pageIndex);
    while (true) {
      OCacheEntry cacheEntry = null;

      if (checkPinnedPages) {
        cacheEntry = pinnedPages.get(pageKey);
      }

      if (cacheEntry != null) {
        if (cacheEntry.acquirePinnedEntry()) {
          return cacheEntry;
        }

        continue;
      }

      cacheEntry = data.get(new PageKey(fileId, pageIndex));

      if (cacheEntry != null) {
        if (cacheEntry.acquireEntry()) {
          afterRead(cacheEntry);
          return cacheEntry;
        }
      } else {
        final OModifiableBoolean read = new OModifiableBoolean();

        cacheEntry = data.compute(pageKey, (page, entry) -> {
          if (entry == null) {
            try {
              final OCachePointer[] pointers = writeCache
                  .load(fileId, pageIndex, 1, addNewPages, new OModifiableBoolean(), verifyChecksums);

              if (pointers.length == 0) {
                return null;
              }

              return new OCacheEntryImpl(page.fileId, page.pageIndex, pointers[1], false);
            } catch (IOException e) {
              throw OException
                  .wrapException(new OStorageException("Error during loading of page " + pageIndex + " for file " + fileId), e);
            }
          } else {
            read.setValue(true);
            return entry;
          }
        });

        if (cacheEntry == null) {
          return null;
        }

        if (cacheEntry.acquireEntry()) {
          if (read.getValue()) {
            afterRead(cacheEntry);
          } else {
            afterAdd(cacheEntry);
          }

          return cacheEntry;
        }
      }
    }
  }

  private void afterAdd(OCacheEntry entry) {

  }

  private void afterRead(OCacheEntry entry) {

  }

  private void afterRemove(OCacheEntry entry) {

  }

  private void scheduleDrainBuffers() {

  }

  private OReadersWriterSpinLock getFileLock(long fileId) {
    return fileLocks.computeIfAbsent(fileId, (key) -> new OReadersWriterSpinLock());
  }

  @Override
  public void releaseFromRead(OCacheEntry cacheEntry, OWriteCache writeCache) {
    cacheEntry.releaseSharedLock();
    cacheEntry.releaseEntry();
  }

  @Override
  public void releaseFromWrite(OCacheEntry cacheEntry, OWriteCache writeCache) {
    cacheLock.acquireReadLock();
    try {
      OReadersWriterSpinLock fileLock = fileLocks.get(cacheEntry.getFileId());
      fileLock.acquireReadLock();
      try {
        final CountDownLatch[] latch = new CountDownLatch[1];

        final OCachePointer cachePointer = cacheEntry.getCachePointer();
        assert cachePointer != null;

        final PageKey pageKey = new PageKey(cacheEntry.getFileId(), (int) cacheEntry.getPageIndex());
        if (cacheEntry.isDirty()) {
          data.compute(pageKey, (page, entry) -> {

            latch[0] = writeCache.store(cacheEntry.getFileId(), cacheEntry.getPageIndex(), cacheEntry.getCachePointer());

            return entry;//may be absent if page in pinned pages, in such case we use map as virtual lock
          });

          cacheEntry.clearDirty();
        }
        cacheEntry.releaseEntry();

        //We need to release exclusive lock from cache pointer after we put it into the write cache so both "dirty pages" of write
        //cache and write cache itself will contain actual values simultaneously. But because cache entry can be cleared after we put it back to the
        //read cache we make copy of cache pointer before head.
        //
        //Following situation can happen, if we release exclusive lock before we put entry to the write cache.
        //1. Page is loaded for write, locked and related LSN is written to the "dirty pages" table.
        //2. Page lock is released.
        //3. Page is chosen to be flushed on disk and its entry removed from "dirty pages" table
        //4. Page is added to write cache as dirty
        //
        //So we have situation when page is added as dirty into the write cache but its related entry in "dirty pages" table is removed
        //it is treated as flushed during fuzzy checkpoint and portion of write ahead log which contains not flushed changes is removed.
        //This can lead to the data loss after restore and corruption of data structures
        cachePointer.releaseExclusiveLock();

        if (latch[0] != null) {
          try {
            latch[0].await();
          } catch (InterruptedException e) {
            Thread.interrupted();
            throw OException.wrapException(new OInterruptedException("File flush was interrupted"), e);
          } catch (Exception e) {
            throw OException.wrapException(new OReadCacheException("File flush was abnormally terminated"), e);
          }
        }
      } finally {
        fileLock.releaseReadLock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }

  }

  @Override
  public void pinPage(OCacheEntry cacheEntry) {
    if ((100 * (pinnedPagesSize.get() + cacheEntry.getSize())) / maxSize.get() > percentOfPinnedPages) {
      if (pinnedPagesWarningCounter.get() < MAX_AMOUNT_OF_WARNINGS_PINNED_PAGES) {

        final long warnings = pinnedPagesWarningCounter.getAndIncrement();
        if (warnings < MAX_AMOUNT_OF_WARNINGS_PINNED_PAGES) {
          OLogManager.instance().warn(this, "Maximum amount of pinned pages is reached, given page " + cacheEntry
              + " will not be marked as pinned which may lead to performance degradation. You may consider to increase the percent of pinned pages "
              + "by changing the property '" + OGlobalConfiguration.DISK_CACHE_PINNED_PAGES.getKey() + "'");
        }
      }

      return;
    }

    final boolean[] added = new boolean[1];
    final PageKey pageKey = new PageKey(cacheEntry.getFileId(), (int) cacheEntry.getPageIndex());
    cacheLock.acquireReadLock();
    try {
      OReadersWriterSpinLock fileLock = getFileLock(cacheEntry.getFileId());
      fileLock.acquireReadLock();
      try {
        data.compute(pageKey, (page, entry) -> {
          if (entry != null) {
            final OCacheEntry updated = pinnedPages.putIfAbsent(pageKey, entry);
            if (updated == null) {
              maxSize.addAndGet(-entry.getSize());
              added[0] = true;
            }
          }

          return null;
        });
      } finally {
        fileLock.releaseReadLock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    pinnedPagesSize.addAndGet(cacheEntry.getSize());
    if (added[0]) {
      afterRemove(cacheEntry);
    }
  }

  @Override
  public OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache, boolean verifyChecksums) throws IOException {
    OCacheEntry cacheEntry;

    final OReadersWriterSpinLock fileLock = getFileLock(fileId);
    fileLock.acquireWriteLock();
    try {
      final long filledUpTo = writeCache.getFilledUpTo(fileId);
      assert filledUpTo >= 0;

      cacheEntry = doLoad(fileId, (int) filledUpTo, false, writeCache, true, true);
    } finally {
      fileLock.releaseWriteLock();
    }

    if (cacheEntry != null) {
      cacheEntry.acquireExclusiveLock();
      writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer());
    }

    return cacheEntry;
  }

  @Override
  public long getUsedMemory() {
    return 0;
  }

  @Override
  public void clear() {
    cacheLock.acquireWriteLock();
    try {
      for (OCacheEntry entry : data.values()) {
        if (entry.isReleased()) {
          final OCachePointer cachePointer = entry.getCachePointer();
          cachePointer.decrementReadersReferrer();
          entry.clearCachePointer();
        } else {
          throw new OStorageException(
              "Page with index " + entry.getPageIndex() + " for file id " + entry.getFileId() + " is used and cannot be removed");
        }
      }

      clearPinnedPages();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  private void clearPinnedPages() {
    for (OCacheEntry pinnedEntry : pinnedPages.values()) {
      if (pinnedEntry.isReleased()) {
        final OCachePointer cachePointer = pinnedEntry.getCachePointer();
        cachePointer.decrementReadersReferrer();
        pinnedEntry.clearCachePointer();

        pinnedPagesSize.addAndGet(-pinnedEntry.getSize());
      } else
        throw new OStorageException("Page with index " + pinnedEntry.getPageIndex() + " for file with id " + pinnedEntry.getFileId()
            + "cannot be freed because it is used.");
    }

    pinnedPages.clear();
  }

  @Override
  public void truncateFile(long fileId, OWriteCache writeCache) throws IOException {

  }

  @Override
  public void closeFile(long fileId, boolean flush, OWriteCache writeCache) {

  }

  @Override
  public void deleteFile(long fileId, OWriteCache writeCache) throws IOException {

  }

  @Override
  public void deleteStorage(OWriteCache writeCache) throws IOException {

  }

  @Override
  public void closeStorage(OWriteCache writeCache) throws IOException {

  }

  @Override
  public void loadCacheState(OWriteCache writeCache) {
    //TODO: implement at final stage
  }

  @Override
  public void storeCacheState(OWriteCache writeCache) {
    //TODO: implement at final stage
  }

  private static final class PageKey {
    private final long fileId;
    private final int  pageIndex;

    private int hash;

    PageKey(long fileId, int pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      PageKey pageKey = (PageKey) o;
      return fileId == pageKey.fileId && pageIndex == pageKey.pageIndex;
    }

    @Override
    public int hashCode() {
      if (hash != 0) {
        return hash;
      }

      return hash = Objects.hash(fileId, pageIndex);
    }

    @Override
    public String toString() {
      return "PageKey{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
    }
  }
}

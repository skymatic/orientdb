package com.orientechnologies.orient.core.storage.cache.local.wtinylfu;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OReadCacheException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction.FrequencySketch;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction.WTinyLFU;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.readbuffer.BoundedBuffer;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.readbuffer.Buffer;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.writequeue.MPSCLinkedQueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class WTinyLFUReadCache implements OReadCache {
  /**
   * Maximum amount of times when we will show message that limit of pinned pages was exhausted.
   */
  private static final int MAX_AMOUNT_OF_WARNINGS_PINNED_PAGES = 10;
  private static final int NCPU                                = Runtime.getRuntime().availableProcessors();
  private static final int WRITE_BUFFER_MAX_BATCH              = 128 * ceilingPowerOfTwo(NCPU);

  private final ConcurrentHashMap<PageKey, OCacheEntry> data;
  private final ConcurrentHashMap<PageKey, OCacheEntry> pinnedPages = new ConcurrentHashMap<>();

  private final AtomicInteger pinnedPagesSize = new AtomicInteger();
  private final int           percentOfPinnedPages;

  private final WTinyLFU wTinyLFU;
  private final Lock     evictionLock = new ReentrantLock();

  private final Buffer<OCacheEntry>       readBuffer  = new BoundedBuffer<>();
  private final MPSCLinkedQueue<Runnable> writeBuffer = new MPSCLinkedQueue<>();

  /**
   * Status which indicates whether flush of buffers should be performed or may be delayed.
   */
  private final AtomicReference<DrainStatus> drainStatus = new AtomicReference<>(DrainStatus.IDLE);

  /**
   * Counts how much time we warned user that limit of amount of pinned pages is reached.
   */
  private final AtomicInteger pinnedPagesWarningCounter = new AtomicInteger();

  public WTinyLFUReadCache(int maxCacheSize, int percentOfPinnedPages) {
    evictionLock.lock();
    try {
      this.data = new ConcurrentHashMap<>(maxCacheSize);
      this.percentOfPinnedPages = percentOfPinnedPages;
      wTinyLFU = new WTinyLFU(data, new FrequencySketch<>());
      wTinyLFU.setMaxSize(maxCacheSize);
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public long addFile(String fileName, OWriteCache writeCache) throws IOException {
    return writeCache.addFile(fileName);
  }

  @Override
  public long addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException {
    return writeCache.addFile(fileName, fileId);
  }

  @Override
  public OCacheEntry loadForWrite(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums) {
    final OCacheEntry cacheEntry = doLoad(fileId, (int) pageIndex, checkPinnedPages, writeCache, false, verifyChecksums);

    if (cacheEntry != null) {
      cacheEntry.acquireExclusiveLock();
      writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer());
    }

    return cacheEntry;
  }

  @Override
  public OCacheEntry loadForRead(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums) {
    final OCacheEntry cacheEntry = doLoad(fileId, (int) pageIndex, checkPinnedPages, writeCache, false, verifyChecksums);
    return cacheEntry;
  }

  private OCacheEntry doLoad(long fileId, int pageIndex, boolean checkPinnedPages, OWriteCache writeCache, boolean addNewPages,
      boolean verifyChecksums) {

    final PageKey pageKey = new PageKey(fileId, pageIndex);
    while (true) {
      checkWriteBuffer();

      OCacheEntry cacheEntry = null;

      if (checkPinnedPages) {
        cacheEntry = pinnedPages.get(pageKey);
      }

      if (cacheEntry != null) {
        if (cacheEntry.makePinned()) {
          return cacheEntry;
        }

        continue;
      }

      cacheEntry = data.get(pageKey);

      if (cacheEntry != null) {
        if (cacheEntry.acquireEntry()) {
          afterRead(cacheEntry);
          return cacheEntry;
        }
      } else {
        final boolean[] read = new boolean[1];

        cacheEntry = data.compute(pageKey, (page, entry) -> {
          if (entry == null) {
            try {
              final OCachePointer[] pointers = writeCache
                  .load(fileId, pageIndex, 1, addNewPages, new OModifiableBoolean(), verifyChecksums);

              if (pointers.length == 0) {
                return null;
              }

              return new OCacheEntryImpl(page.getFileId(), page.getPageIndex(), pointers[1], false);
            } catch (IOException e) {
              throw OException
                  .wrapException(new OStorageException("Error during loading of page " + pageIndex + " for file " + fileId), e);
            }
          } else {
            read[0] = true;
            return entry;
          }
        });

        if (cacheEntry == null) {
          return null;
        }

        if (cacheEntry.acquireEntry()) {
          if (read[0]) {
            afterRead(cacheEntry);
          } else {
            afterAdd(cacheEntry);
          }

          return cacheEntry;
        }
      }
    }
  }

  @Override
  public void releaseFromRead(OCacheEntry cacheEntry, OWriteCache writeCache) {
    if (!cacheEntry.isPinned()) {
      cacheEntry.releaseEntry();
    }
  }

  @Override
  public void releaseFromWrite(OCacheEntry cacheEntry, OWriteCache writeCache) {
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

    if (!cacheEntry.isPinned()) {
      cacheEntry.releaseEntry();
    }

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
  }

  @Override
  public void pinPage(OCacheEntry cacheEntry) {
    if ((100 * (pinnedPagesSize.get() + cacheEntry.getSize())) / wTinyLFU.getMaxSize() > percentOfPinnedPages) {
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

    assert !cacheEntry.isReleased();

    final PageKey pageKey = new PageKey(cacheEntry.getFileId(), (int) cacheEntry.getPageIndex());
    pinnedPages.putIfAbsent(pageKey, cacheEntry);

    final OCacheEntry removed = data.remove(pageKey);
    if (removed != null) {
      afterPining(removed);
    }
  }

  @Override
  public OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache, boolean verifyChecksums) throws IOException {
    final int newPageIndex = writeCache.allocateNewPage(fileId);
    final OCacheEntry cacheEntry = doLoad(fileId, newPageIndex, false, writeCache, true, true);

    if (cacheEntry != null) {
      cacheEntry.acquireExclusiveLock();
      writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer());
    }

    return cacheEntry;
  }

  private void afterRead(OCacheEntry entry) {
    final boolean bufferOverflow = readBuffer.offer(entry) == Buffer.FULL;

    if (drainStatus.get().shouldBeDrained(bufferOverflow)) {
      tryToDrainBuffers();
    }
  }

  private void afterAdd(OCacheEntry entry) {
    afterWrite(() -> wTinyLFU.onAdd(entry));
  }

  private void afterPining(OCacheEntry entry) {
    afterWrite(() -> wTinyLFU.onPinning(entry));
  }

  private void afterWrite(Runnable command) {
    writeBuffer.offer(command);

    if (drainStatus.get() == DrainStatus.IDLE) {
      drainStatus.compareAndSet(DrainStatus.IDLE, DrainStatus.REQUIRED);
    }

    tryToDrainBuffers();
  }

  private void checkWriteBuffer() {
    if (!writeBuffer.isEmpty()) {

      if (drainStatus.get() == DrainStatus.IDLE) {
        drainStatus.compareAndSet(DrainStatus.IDLE, DrainStatus.REQUIRED);
      }

      tryToDrainBuffers();
    }
  }

  private void tryToDrainBuffers() {
    if (drainStatus.get() == DrainStatus.IN_PROGRESS) {
      return;
    }

    if (evictionLock.tryLock()) {
      try {
        //optimization to avoid to call tryLock if it is not needed
        drainStatus.lazySet(DrainStatus.IN_PROGRESS);
        drainBuffers();
      } finally {
        //cas operation because we do not want to overwrite REQUIRED status and to avoid false optimization of
        //drain buffer by IN_PROGRESS status
        drainStatus.compareAndSet(DrainStatus.IN_PROGRESS, DrainStatus.IDLE);
        evictionLock.unlock();
      }
    }
  }

  private void drainBuffers() {
    drainWriteBuffer();
    drainReadBuffers();
  }

  private void drainReadBuffers() {
    readBuffer.drainTo(wTinyLFU::onAccess);
  }

  private void drainWriteBuffer() {
    for (int i = 0; i < WRITE_BUFFER_MAX_BATCH; i++) {
      final Runnable command = writeBuffer.poll();

      if (command == null) {
        break;
      }

      command.run();
    }
  }

  @Override
  public long getUsedMemory() {
    return wTinyLFU.getSize() * 4 * 1024;
  }

  @Override
  public void clear() {
    evictionLock.lock();
    try {
      for (OCacheEntry entry : data.values()) {
        if (entry.freeze()) {
          wTinyLFU.onRemove(entry);
        } else {
          throw new OStorageException(
              "Page with index " + entry.getPageIndex() + " for file id " + entry.getFileId() + " is used and cannot be removed");
        }
      }

      clearPinnedPages();
    } finally {
      evictionLock.unlock();
    }
  }

  private void clearPinnedPages() {
    for (OCacheEntry pinnedEntry : pinnedPages.values()) {
      final OCachePointer cachePointer = pinnedEntry.getCachePointer();
      cachePointer.decrementReadersReferrer();
      pinnedEntry.clearCachePointer();

      pinnedPagesSize.addAndGet(-pinnedEntry.getSize());
    }

    pinnedPages.clear();
  }

  @Override
  public void truncateFile(long fileId, OWriteCache writeCache) throws IOException {
    final int filledUpTo = (int) writeCache.getFilledUpTo(fileId);
    writeCache.truncateFile(fileId);

    clearFile(fileId, filledUpTo);
  }

  private void clearFile(long fileId, int filledUpTo) {
    evictionLock.lock();
    try {
      for (int pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
        final PageKey pageKey = new PageKey(fileId, pageIndex);

        OCacheEntry cacheEntry = pinnedPages.remove(pageKey);

        if (cacheEntry != null) {
          OCachePointer cachePointer = cacheEntry.getCachePointer();

          cachePointer.incrementReadersReferrer();
          cacheEntry.clearCachePointer();
        } else {
          cacheEntry = data.remove(pageKey);

          if (cacheEntry.freeze()) {
            wTinyLFU.onRemove(cacheEntry);
          } else {
            throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file id " + cacheEntry.getFileId()
                + " is used and cannot be removed");
          }
        }
      }
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public void closeFile(long fileId, boolean flush, OWriteCache writeCache) {
    final int filledUpTo = (int) writeCache.getFilledUpTo(fileId);

    writeCache.close(fileId, flush);
    clearFile(fileId, filledUpTo);
  }

  @Override
  public void deleteFile(long fileId, OWriteCache writeCache) throws IOException {
    final int filledUpTo = (int) writeCache.getFilledUpTo(fileId);
    clearFile(fileId, filledUpTo);

    writeCache.deleteFile(fileId);
  }

  @Override
  public void deleteStorage(OWriteCache writeCache) throws IOException {
    final Collection<Long> files = writeCache.files().values();
    final Map<Long, Integer> filledUpTo = new HashMap<>();
    for (long fileId : files) {
      filledUpTo.put(fileId, (int) writeCache.getFilledUpTo(fileId));
    }

    final long[] filesToClear = writeCache.delete();
    for (long fileId : filesToClear) {
      clearFile(fileId, filledUpTo.get(fileId));
    }
  }

  @Override
  public void closeStorage(OWriteCache writeCache) throws IOException {
    final long[] filesToClear = writeCache.close();

    for (long fileId : filesToClear) {
      final int filledUpTo = (int) writeCache.getFilledUpTo(fileId);
      clearFile(fileId, filledUpTo);
    }
  }

  @Override
  public void loadCacheState(OWriteCache writeCache) {
    //TODO: implement at final stage
  }

  @Override
  public void storeCacheState(OWriteCache writeCache) {
    //TODO: implement at final stage
  }

  private enum DrainStatus {
    IDLE {
      @Override
      boolean shouldBeDrained(boolean readBufferOverflow) {
        return readBufferOverflow;
      }
    }, IN_PROGRESS {
      @Override
      boolean shouldBeDrained(boolean readBufferOverflow) {
        return false;
      }
    }, REQUIRED {
      @Override
      boolean shouldBeDrained(boolean readBufferOverflow) {
        return true;
      }
    };

    abstract boolean shouldBeDrained(boolean readBufferOverflow);
  }

  private static int ceilingPowerOfTwo(int x) {
    // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
    return 1 << -Integer.numberOfLeadingZeros(x - 1);
  }
}

package com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.wtinylfu.PageKey;

import java.util.concurrent.ConcurrentHashMap;

public final class WTinyLFU {
  private static final int EDEN_PERCENT         = 10;
  private static final int PROBATIONARY_PERCENT = 20;

  private volatile int                                     size;
  private volatile int                                     maxSize;
  private final    ConcurrentHashMap<PageKey, OCacheEntry> data;
  private final    Admittor<PageKey>                       admittor;

  private final LRUList eden       = new LRUList();
  private final LRUList probation  = new LRUList();
  private final LRUList protection = new LRUList();

  private int maxEdenSize;
  private int maxProbationarySize;
  private int maxProtectedSize;

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
    admittor.increment(new PageKey(cacheEntry.getFileId(), (int) cacheEntry.getPageIndex()));

    if (!cacheEntry.isDead()) {
      if (probation.contains(cacheEntry)) {
        probation.remove(cacheEntry);
        protection.moveToTheTail(cacheEntry);

        if (protection.size() > maxProtectedSize) {
          cacheEntry = protection.poll();

          probation.moveToTheTail(cacheEntry);
        }
      } else if (protection.contains(cacheEntry)) {
        protection.moveToTheTail(cacheEntry);
      } else if (eden.contains(cacheEntry)) {
        eden.moveToTheTail(cacheEntry);
      } else {
        //that is possible if page is pinned so OK.
      }
    }

    assert eden.size() <= maxEdenSize;
    assert protection.size() <= maxProtectedSize;
    assert probation.size() <= maxProbationarySize;
  }

  public void onAdd(OCacheEntry cacheEntry) {
    //it is always alive because we remove pages only inside of eviction lock,
    //and always pin only added pages
    assert cacheEntry.isAlive();

    assert !eden.contains(cacheEntry);
    assert !probation.contains(cacheEntry);
    assert !protection.contains(cacheEntry);

    admittor.increment(new PageKey(cacheEntry.getFileId(), (int) cacheEntry.getPageIndex()));
    eden.moveToTheTail(cacheEntry);

    while (eden.size() > maxEdenSize) {
      final OCacheEntry candidate = eden.poll();
      assert candidate != null;

      if (probation.size() < maxProbationarySize) {
        probation.moveToTheTail(candidate);
      } else {
        final OCacheEntry victim = probation.peek();

        final PageKey candidateKey = new PageKey(candidate.getFileId(), (int) candidate.getPageIndex());
        final PageKey victimKey = new PageKey(victim.getFileId(), (int) victim.getPageIndex());

        final int candidateFrequency = admittor.frequency(candidateKey);
        final int victimFrequency = admittor.frequency(victimKey);

        if (candidateFrequency > victimFrequency) {
          probation.poll();

          if (victim.freeze()) {
            data.remove(victimKey, victim);
            victim.makeDead();

            //noinspection NonAtomicOperationOnVolatileField
            size -= victim.getSize();

            final OCachePointer pointer = victim.getCachePointer();

            pointer.decrementReadersReferrer();
            victim.clearCachePointer();
          } else {
            eden.moveToTheTail(victim);
          }
        } else {
          if (candidate.freeze()) {
            data.remove(candidateKey, candidate);
            candidate.makeDead();

            //noinspection NonAtomicOperationOnVolatileField
            size -= candidate.getSize();

            final OCachePointer pointer = candidate.getCachePointer();

            pointer.decrementReadersReferrer();
            candidate.clearCachePointer();
          } else {
            eden.moveToTheTail(candidate);
          }
        }
      }
    }

    //noinspection NonAtomicOperationOnVolatileField
    size += cacheEntry.getSize();

    assert size <= maxSize;
    assert eden.size() <= maxEdenSize;
    assert protection.size() <= maxProtectedSize;
    assert probation.size() <= maxProbationarySize;
  }

  public void onPinning(OCacheEntry cacheEntry) {
    //entry already can be removed after addition, but pinning can not be done before addition, so we
    //do not check/have additional status to signal that page is removed from data map but not removed yet from metadata.
    //So we do not care about interleaving of removals and additions, because we always remove(pin) page after addition.
    if (!cacheEntry.isDead()) {
      if (probation.contains(cacheEntry)) {
        probation.remove(cacheEntry);
      } else if (protection.contains(cacheEntry)) {
        protection.remove(cacheEntry);
      } else if (eden.contains(cacheEntry)) {
        eden.remove(cacheEntry);
      } else {
        throw new IllegalStateException("At least one LRU list should contain entry");
      }
    }

    //noinspection NonAtomicOperationOnVolatileField
    size -= cacheEntry.getSize();
    //noinspection NonAtomicOperationOnVolatileField
    maxSize -= cacheEntry.getSize();

    assert size > 0;
    assert maxSize > 0;

    calculateMaxSizes();

    assert eden.size() <= maxEdenSize;
    assert protection.size() <= maxProtectedSize;
    assert probation.size() <= maxProbationarySize;
  }

  public void onRemove(OCacheEntry cacheEntry) {
    assert cacheEntry.isAlive();

    if (probation.contains(cacheEntry)) {
      probation.remove(cacheEntry);
    } else if (protection.contains(cacheEntry)) {
      protection.remove(cacheEntry);
    } else if (eden.contains(cacheEntry)) {
      eden.remove(cacheEntry);
    } else {
      throw new IllegalStateException("At least one LRU list should contain entry");
    }

    cacheEntry.makeDead();
    //noinspection NonAtomicOperationOnVolatileField
    size -= cacheEntry.getSize();

    final OCachePointer cachePointer = cacheEntry.getCachePointer();
    cachePointer.decrementReadersReferrer();
    cacheEntry.clearCachePointer();
  }

  private void calculateMaxSizes() {
    maxEdenSize = maxSize * EDEN_PERCENT / 100;
    maxProbationarySize = (maxSize - maxEdenSize) * PROBATIONARY_PERCENT / 100;
    maxProtectedSize = maxSize - maxEdenSize - maxProbationarySize;
  }
}

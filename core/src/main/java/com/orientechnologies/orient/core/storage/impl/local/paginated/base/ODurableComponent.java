/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OUpdatePageRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.IOException;

/**
 * Base class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * This class contains methods which are used to support such concepts as:
 * <ol>
 * <li>"atomic operation" - set of operations which should be either applied together or not. It includes not only changes on
 * current data structure but on all durable data structures which are used by current one during implementation of specific
 * operation.</li>
 * <li>write ahead log - log of all changes which were done with page content after loading it from cache.</li>
 * </ol>
 * To support of "atomic operation" concept following should be done:
 * <ol>
 * <li>Call {@link #startAtomicOperation(boolean)} method.</li>
 * <li>Call {@link #endAtomicOperation(boolean, Exception)} method when atomic operation completes, passed in parameter should be
 * <code>false</code> if atomic operation completes with success and <code>true</code> if there were some exceptions and it is
 * needed to rollback given operation.</li>
 * </ol>
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/27/13
 */
public abstract class ODurableComponent extends OSharedResourceAdaptive {
  protected final OAtomicOperationsManager     atomicOperationsManager;
  protected final OAbstractPaginatedStorage    storage;
  protected final OReadCache                   readCache;
  protected final OWriteCache                  writeCache;
  protected final OPerformanceStatisticManager performanceStatisticManager;
  private final   OWriteAheadLog               writeAheadLog;

  private volatile String name;
  private volatile String fullName;

  private final String extension;

  private volatile String lockName;

  public ODurableComponent(OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(true);

    assert name != null;
    this.extension = extension;
    this.storage = storage;
    this.fullName = name + extension;
    this.name = name;
    this.atomicOperationsManager = storage.getAtomicOperationsManager();
    this.readCache = storage.getReadCache();
    this.writeCache = storage.getWriteCache();
    this.writeAheadLog = storage.getWALInstance();
    this.performanceStatisticManager = storage.getPerformanceStatisticManager();
    this.lockName = lockName;
  }

  public String getLockName() {
    return lockName;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
    this.fullName = name + extension;
  }

  public String getFullName() {
    return fullName;
  }

  public String getExtension() {
    return extension;
  }

  protected void endAtomicOperation(boolean rollback, Exception e) throws IOException {
    atomicOperationsManager.endAtomicOperation(rollback, e);
  }

  /**
   * @see OAtomicOperationsManager#startAtomicOperation(com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent, boolean)
   */
  protected OAtomicOperation startAtomicOperation(boolean trackNonTxOperations) throws IOException {
    return atomicOperationsManager.startAtomicOperation(this, trackNonTxOperations);
  }

  protected long getFilledUpTo(OAtomicOperation atomicOperation, long fileId) throws IOException {
    return writeCache.getFilledUpTo(fileId);
  }

  protected OCacheEntry loadPageForWrite(final OAtomicOperation atomicOperation, final long fileId, final long pageIndex,
      final boolean checkPinnedPages) throws IOException {
    return loadPageForWrite(atomicOperation, fileId, pageIndex, checkPinnedPages, 1);
  }

  protected OCacheEntry loadPageForWrite(OAtomicOperation atomicOperation, long fileId, long pageIndex, boolean checkPinnedPages,
      final int pageCount) throws IOException {
    return readCache.loadForWrite(fileId, pageIndex, checkPinnedPages, writeCache, 1, true);
  }

  protected OCacheEntry loadPageForRead(final OAtomicOperation atomicOperation, final long fileId, final long pageIndex,
      final boolean checkPinnedPages) throws IOException {
    return loadPageForRead(atomicOperation, fileId, pageIndex, checkPinnedPages, 1);
  }

  protected OCacheEntry loadPageForRead(OAtomicOperation atomicOperation, long fileId, long pageIndex, boolean checkPinnedPages,
      final int pageCount) throws IOException {
    return readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, true);
  }

  protected void pinPage(OAtomicOperation atomicOperation, OCacheEntry cacheEntry) {
    readCache.pinPage(cacheEntry);
  }

  protected OCacheEntry addPage(OAtomicOperation atomicOperation, long fileId) throws IOException {
    return readCache.allocateNewPage(fileId, writeCache, true);
  }

  protected void releasePageFromWrite(OAtomicOperation atomicOperation, ODurablePage page) throws IOException {
    if (page == null) {
      return;
    }

    OCacheEntry cacheEntry = page.getCacheEntry();

    if (atomicOperation != null) {
      OLogSequenceNumber lsn = writeAheadLog.log(new OUpdatePageRecord(cacheEntry.getPageIndex(), cacheEntry.getFileId(),
          atomicOperation.getOperationUnitId(), page.getChanges(), atomicOperation.getLastLsn()));
      page.setLsn(lsn);
      atomicOperation.setLastLsn(lsn);
    }

    readCache.releaseFromWrite(cacheEntry, writeCache);
  }

  protected void releasePageFromRead(OAtomicOperation atomicOperation, OCacheEntry cacheEntry) {
    readCache.releaseFromRead(cacheEntry, writeCache);
  }

  protected long addFile(OAtomicOperation atomicOperation, String fileName) throws IOException {
    return readCache.addFile(fileName, writeCache);
  }

  protected long openFile(OAtomicOperation atomicOperation, String fileName) throws IOException {
    return writeCache.loadFile(fileName);
  }

  protected void deleteFile(OAtomicOperation atomicOperation, long fileId) throws IOException {
    readCache.deleteFile(fileId, writeCache);
  }

  protected boolean isFileExists(OAtomicOperation atomicOperation, String fileName) {
    return writeCache.exists(fileName);
  }

  protected boolean isFileExists(OAtomicOperation atomicOperation, long fileId) {
    return writeCache.exists(fileId);
  }

  protected void truncateFile(OAtomicOperation atomicOperation, long filedId) throws IOException {
    readCache.truncateFile(filedId, writeCache);
  }

  protected void startOperation() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();
    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic
          .startComponentOperation(getFullName(), OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    }
  }

  protected void completeOperation() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();
    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.completeComponentOperation();
    }
  }

}

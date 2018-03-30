package com.orientechnologies.orient.core.storage.cache.local.wtinylfu;

import java.util.Objects;

public final class PageKey {
  private final long fileId;
  private final int  pageIndex;

  private int hash;

  public PageKey(long fileId, int pageIndex) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;
  }

  public long getFileId() {
    return fileId;
  }

  public int getPageIndex() {
    return pageIndex;
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

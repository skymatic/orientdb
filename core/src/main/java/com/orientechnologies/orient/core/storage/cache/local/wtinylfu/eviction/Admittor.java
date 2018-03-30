package com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public interface Admittor<E> {
  void ensureCapacity(@Nonnegative long maximumSize);

  int frequency(@Nonnull E e);

  void increment(@Nonnull E e);
}

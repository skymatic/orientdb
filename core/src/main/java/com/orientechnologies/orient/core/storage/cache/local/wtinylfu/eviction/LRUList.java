package com.orientechnologies.orient.core.storage.cache.local.wtinylfu.eviction;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;

public final class LRUList {
  private int size;

  private OCacheEntry head;
  private OCacheEntry tail;

  void remove(OCacheEntry entry) {
    final OCacheEntry next = entry.getNext();
    final OCacheEntry prev = entry.getPrev();

    if (!(next != null || prev != null || entry == head)) {
      return;
    }

    if (prev != null) {
      assert prev.getNext() == entry;
    }

    if (next != null) {
      assert next.getPrev() == entry;
    }

    if (next != null) {
      next.setPrev(prev);
    }

    if (prev != null) {
      prev.setNext(next);
    }

    if (head == entry) {
      assert entry.getPrev() == null;
      head = next;
    }

    if (tail == entry) {
      assert entry.getNext() == null;
      tail = prev;
    }

    entry.setNext(null);
    entry.setPrev(null);
    entry.setContainer(null);

    size--;
  }

  boolean contains(OCacheEntry entry) {
    return entry.getContainer() == this;
  }

  void moveToTheTail(OCacheEntry entry) {
    if (tail == entry) {
      assert entry.getNext() == null;
      return;
    }

    final OCacheEntry next = entry.getNext();
    final OCacheEntry prev = entry.getPrev();

    boolean newEntry = !(next != null || prev != null || entry == head);

    if (prev != null) {
      assert prev.getNext() == entry;
    }

    if (next != null) {
      assert next.getPrev() == entry;
    }

    if (prev != null) {
      prev.setNext(next);
    }

    if (next != null) {
      next.setPrev(prev);
    }

    if (head == entry) {
      assert entry.getPrev() == null;
      head = next;
    }

    entry.setPrev(tail);
    entry.setNext(null);

    if (tail != null) {
      assert tail.getNext() == null;
      tail.setNext(entry);
      tail = entry;
    } else {
      tail = head = entry;
    }

    if (newEntry) {
      entry.setContainer(this);
      size++;
    } else {
      assert entry.getContainer() == this;
    }
  }

  int size() {
    return size;
  }

  OCacheEntry poll() {
    if (head == null) {
      return null;
    }

    final OCacheEntry entry = head;

    OCacheEntry next = head.getNext();
    assert next == null || next.getPrev() == head;

    head = next;
    if (next != null) {
      next.setPrev(null);
    }

    assert head == null || head.getPrev() == null;

    if (head == null) {
      tail = null;
    }

    entry.setNext(null);
    assert entry.getPrev() == null;

    size--;

    return entry;
  }

  OCacheEntry peek() {
    return head;
  }
}

package com.orientechnologies.orient.core.storage.cache.local.wtinylfu.writequeue;

import java.util.concurrent.atomic.AtomicReference;

public final class Node<E> {
  private final AtomicReference<Node<E>> next = new AtomicReference<>();
  private final E item;

  public Node(E item) {
    this.item = item;
  }

  public Node<E> getNext() {
    return next.get();
  }

  public E getItem() {
    return item;
  }

  public void lazySetNext(Node<E> next) {
    this.next.lazySet(next);
  }
}

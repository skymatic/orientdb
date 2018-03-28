package com.orientechnologies.orient.core.storage.cache.local.wtinylfu.writequeue;

import java.util.concurrent.atomic.AtomicReference;

public class MPSCLinkedQueue<E> {
  private final AtomicReference<Node<E>> head = new AtomicReference<>();
  private final AtomicReference<Node<E>> tail = new AtomicReference<>();

  public MPSCLinkedQueue() {
    final Node<E> dummyNode = new Node<>(null);
    head.set(dummyNode);
    tail.set(dummyNode);
  }

  public void offer(E item) {
    final Node<E> newNode = new Node<>(item);
    final Node<E> prev = tail.getAndSet(newNode);

    prev.lazySetNext(newNode);
  }

  public E poll() {
    final Node<E> head = this.head.get();
    Node<E> next;

    if ((next = head.getNext()) != null) {
      this.head.set(next);
      return head.getItem();
    }

    final Node<E> tail = this.tail.get();
    if (head == tail) {
      return null;
    }

    while ((next = head.getNext()) == null) {
      Thread.yield();
    }

    this.head.set(next);

    return head.getItem();
  }
}

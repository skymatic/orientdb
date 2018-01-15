package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class OPaginatedClusterRecordsIteratorTest {
  @Test
  public void testAddIterateRecords() {
    try (OrientDB orientDB = new OrientDB("embedded:OPaginatedClusterRecordsIteratorTest", OrientDBConfig.defaultConfig())) {
      orientDB.create("testAddIterateRecords", ODatabaseType.MEMORY);

      try (ODatabaseSession session = orientDB.open("testAddIterateRecords", "admin", "admin")) {
        final int clusterId = session.addCluster("iteratedCluster");

        final ThreadLocalRandom random = ThreadLocalRandom.current();

        final List<ORecord> savedDocs = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
          for (int k = 0; k < i * 100; k++) {
            final ODocument document = new ODocument();

            final byte[] content = new byte[random.nextInt(2 * 65536) + 10];
            document.field("value", content);

            session.save(document, "iteratedCluster");

            savedDocs.add(document);
          }

          final OPaginatedClusterRecordsIterator<ORecord> docIterator = new OPaginatedClusterRecordsIterator<>(
              (ODatabaseDocumentInternal) session, clusterId);

          final Set<ORecord> recordsToCheck = new HashSet<>(savedDocs);
          while (docIterator.hasNext()) {
            final ORecord record = docIterator.next();
            Assert.assertTrue(recordsToCheck.remove(record));

            for (int c = 0; c < 3; c++) {
              docIterator.hasNext();
            }
          }

          Assert.assertTrue(recordsToCheck.isEmpty());
        }
      }
    }
  }

  @Test
  public void testAddIterateRecordsNoHasNext() {
    try (OrientDB orientDB = new OrientDB("embedded:OPaginatedClusterRecordsIteratorTest", OrientDBConfig.defaultConfig())) {
      orientDB.create("testAddIterateRecordsNoHasNext", ODatabaseType.MEMORY);

      try (ODatabaseSession session = orientDB.open("testAddIterateRecordsNoHasNext", "admin", "admin")) {
        final int clusterId = session.addCluster("iteratedCluster");

        final ThreadLocalRandom random = ThreadLocalRandom.current();

        final List<ORecord> savedDocs = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
          for (int k = 0; k < i * 100; k++) {
            final ODocument document = new ODocument();

            final byte[] content = new byte[random.nextInt(2 * 65536) + 10];
            document.field("value", content);

            session.save(document, "iteratedCluster");

            savedDocs.add(document);
          }

          final OPaginatedClusterRecordsIterator<ORecord> docIterator = new OPaginatedClusterRecordsIterator<>(
              (ODatabaseDocumentInternal) session, clusterId);

          final Set<ORecord> recordsToCheck = new HashSet<>(savedDocs);

          while (true) {
            try {
              final ORecord record = docIterator.next();
              Assert.assertTrue(recordsToCheck.remove(record));
            } catch (NoSuchElementException e) {

              Assert.assertTrue(!docIterator.hasNext());
              Assert.assertTrue(!docIterator.hasNext());
              Assert.assertTrue(!docIterator.hasNext());

              break;
            }
          }

          Assert.assertTrue(recordsToCheck.isEmpty());
        }
      }
    }
  }

  @Test
  public void testAddIterateRecordsInTx() {
    try (OrientDB orientDB = new OrientDB("embedded:OPaginatedClusterRecordsIteratorTest", OrientDBConfig.defaultConfig())) {
      orientDB.create("testAddIterateRecordsInTx", ODatabaseType.MEMORY);

      try (ODatabaseSession session = orientDB.open("testAddIterateRecordsInTx", "admin", "admin")) {
        final int clusterId = session.addCluster("iteratedCluster");

        final ThreadLocalRandom random = ThreadLocalRandom.current();

        final List<ORecord> savedDocs = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {

          session.begin();
          for (int k = 0; k < i * 100; k++) {
            final ODocument document = new ODocument();

            final byte[] content = new byte[random.nextInt(2 * 65536) + 10];
            document.field("value", content);

            session.save(document, "iteratedCluster");

            savedDocs.add(document);
          }

          final OPaginatedClusterRecordsIterator<ORecord> docIterator = new OPaginatedClusterRecordsIterator<>(
              (ODatabaseDocumentInternal) session, clusterId);

          final Set<ORecord> recordsToCheck = new HashSet<>(savedDocs);
          while (docIterator.hasNext()) {
            final ORecord record = docIterator.next();
            Assert.assertTrue(recordsToCheck.remove(record));

            for (int c = 0; c < 3; c++) {
              docIterator.hasNext();
            }
          }

          Assert.assertTrue(recordsToCheck.isEmpty());
          session.commit();
        }

        final OPaginatedClusterRecordsIterator<ORecord> docIterator = new OPaginatedClusterRecordsIterator<>(
            (ODatabaseDocumentInternal) session, clusterId);

        final Set<ORecord> recordsToCheck = new HashSet<>(savedDocs);
        while (docIterator.hasNext()) {
          final ORecord record = docIterator.next();
          Assert.assertTrue(recordsToCheck.remove(record));

          for (int c = 0; c < 3; c++) {
            docIterator.hasNext();
          }
        }
      }
    }
  }

  @Test
  public void testAddIterateRecordsNoHasNextInTx() {
    try (OrientDB orientDB = new OrientDB("embedded:OPaginatedClusterRecordsIteratorTest", OrientDBConfig.defaultConfig())) {
      orientDB.create("testAddIterateRecordsNoHasNextInTx", ODatabaseType.MEMORY);

      try (ODatabaseSession session = orientDB.open("testAddIterateRecordsNoHasNextInTx", "admin", "admin")) {
        final int clusterId = session.addCluster("iteratedCluster");

        final ThreadLocalRandom random = ThreadLocalRandom.current();

        final List<ORecord> savedDocs = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
          session.begin();
          for (int k = 0; k < i * 100; k++) {
            final ODocument document = new ODocument();

            final byte[] content = new byte[random.nextInt(2 * 65536) + 10];
            document.field("value", content);

            session.save(document, "iteratedCluster");

            savedDocs.add(document);
          }

          final OPaginatedClusterRecordsIterator<ORecord> docIterator = new OPaginatedClusterRecordsIterator<>(
              (ODatabaseDocumentInternal) session, clusterId);

          final Set<ORecord> recordsToCheck = new HashSet<>(savedDocs);

          while (true) {
            try {
              final ORecord record = docIterator.next();
              Assert.assertTrue(recordsToCheck.remove(record));
            } catch (NoSuchElementException e) {

              Assert.assertTrue(!docIterator.hasNext());
              Assert.assertTrue(!docIterator.hasNext());
              Assert.assertTrue(!docIterator.hasNext());

              break;
            }
          }

          Assert.assertTrue(recordsToCheck.isEmpty());
          session.commit();
        }
      }
    }
  }

}

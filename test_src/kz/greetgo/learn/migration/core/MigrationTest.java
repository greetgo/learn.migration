package kz.greetgo.learn.migration.core;

import kz.greetgo.learn.migration.interfaces.ConnectionConfig;
import kz.greetgo.learn.migration.util.ConfigFiles;
import kz.greetgo.learn.migration.util.ConnectionUtils;

public class MigrationTest {

  public static void main(String[] args) throws Exception {

    ConnectionConfig operCC = ConnectionUtils.fileToConnectionConfig(ConfigFiles.operDb());
    ConnectionConfig ciaCC = ConnectionUtils.fileToConnectionConfig(ConfigFiles.ciaDb());

    try (Migration migration = new Migration(operCC, ciaCC)) {

      migration.portionSize = 1_000_000;
      migration.uploadMaxBatchSize = 50_000;

      while (true) {
        int count = migration.migrate();
        if (count == 0) break;
        System.out.println("Migrated " + count + " records");
        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------");
      }
    }

    System.out.println("Finish migration");
  }

}
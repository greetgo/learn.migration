package kz.greetgo.learn.migration.core;

import kz.greetgo.learn.migration.interfaces.ConnectionConfig;
import kz.greetgo.learn.migration.util.ConfigFiles;
import kz.greetgo.learn.migration.util.ConnectionUtils;

import java.io.File;

public class LaunchMigration {

  public static void main(String[] args) throws Exception {

    final File file = new File("build/__migration__");
    file.getParentFile().mkdirs();
    file.createNewFile();

    System.out.println("To stop next migration portion delete file " + file);
    System.out.println("To stop next migration portion delete file " + file);
    System.out.println("To stop next migration portion delete file " + file);

    ConnectionConfig operCC = ConnectionUtils.fileToConnectionConfig(ConfigFiles.operDb());
    ConnectionConfig ciaCC = ConnectionUtils.fileToConnectionConfig(ConfigFiles.ciaDb());

    try (Migration migration = new Migration(operCC, ciaCC)) {

      migration.portionSize = 250_000;
      migration.uploadMaxBatchSize = 50_000;
      migration.downloadMaxBatchSize = 50_000;

      while (true) {
        int count = migration.migrate();
        if (count == 0) break;
        if (!file.exists()) break;
        System.out.println("Migrated " + count + " records");
        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------");
      }
    }

    file.delete();

    System.out.println("Finish migration");
  }

}
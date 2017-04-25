package kz.greetgo.learn.migration.__prepare__;

import kz.greetgo.learn.migration.__prepare__.core.DbWorker;

public class PrepareOperDb {
  public static void main(String[] args) throws Exception {
    DbWorker dbWorker = new DbWorker();

    dbWorker.prepareConfigFiles();

    dbWorker.dropOperDb();
    dbWorker.createOperDb();

    dbWorker.createMigrationSourceDb();
  }
}

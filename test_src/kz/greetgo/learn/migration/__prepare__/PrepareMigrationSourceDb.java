package kz.greetgo.learn.migration.__prepare__;

import kz.greetgo.learn.migration.__prepare__.core.DbWorker;
import kz.greetgo.learn.migration.__prepare__.db.cia.CiaDDL;
import kz.greetgo.learn.migration.util.ConfigFiles;

public class PrepareMigrationSourceDb {
  public static void main(String[] args) throws Exception {
    DbWorker dbWorker = new DbWorker();

    dbWorker.prepareConfigFiles();

    dbWorker.dropMigrationSourceDb();
    dbWorker.createMigrationSourceDb();

    dbWorker.applyDDL(ConfigFiles.migrationSourceDb(), CiaDDL.get());
  }
}

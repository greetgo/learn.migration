package kz.greetgo.learn.migration.__prepare__;

import kz.greetgo.learn.migration.__prepare__.core.DbWorker;
import kz.greetgo.learn.migration.__prepare__.db.oper.OperDDL;
import kz.greetgo.learn.migration.util.ConfigFiles;

public class DropCreateOperDb {
  public static void main(String[] args) throws Exception {
    DbWorker dbWorker = new DbWorker();

    dbWorker.prepareConfigFiles();

    dbWorker.dropOperDb();
    dbWorker.createOperDb();

    dbWorker.applyDDL(ConfigFiles.operDb(), OperDDL.get());
  }
}

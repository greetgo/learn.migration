package kz.greetgo.learn.migration.__prepare__;

public class PrepareDb {
  public static void main(String[] args) throws Exception {
    DbWorker dbWorker = new DbWorker();

    dbWorker.prepareConfigFiles();

    dbWorker.dropOperDb();
    dbWorker.createOperDb();
  }
}

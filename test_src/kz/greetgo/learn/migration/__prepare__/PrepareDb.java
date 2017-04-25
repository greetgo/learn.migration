package kz.greetgo.learn.migration.__prepare__;

public class PrepareDb {
  public static void main(String[] args) {
    DbWorker dbWorker = new DbWorker();

    dbWorker.prepareConfigFiles();
  }
}

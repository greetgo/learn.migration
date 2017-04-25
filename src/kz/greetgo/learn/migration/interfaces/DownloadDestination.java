package kz.greetgo.learn.migration.interfaces;

public interface DownloadDestination {

  default void start() {
  }

  void record();

  default void finish() {
  }

}

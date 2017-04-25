package kz.greetgo.learn.migration.__prepare__;

import kz.greetgo.learn.migration.util.ConfigFiles;
import kz.greetgo.learn.migration.util.FileUtils;

import java.io.File;

public class DbWorker {

  public void prepareConfigFiles() {
    prepareConfigDbFile(ConfigFiles.operDb(), "learn_migration");
    prepareConfigDbFile(ConfigFiles.migrationSourceDb(), "learn_migration_source");
  }

  private void prepareConfigDbFile(File configFile, String db) {
    if (configFile.exists()) {
      info("File " + configFile + " already exists");
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("url=").append(DbAdminAccess.changeDb(DbAdminAccess.adminUrl(),
      System.getProperty("user.name") + "_" + db)).append('\n');
    sb.append("user=").append(db).append('\n');
    sb.append("password=7777777\n");

    configFile.getParentFile().mkdirs();
    FileUtils.setStrToFile(sb.toString(), configFile);
    info("Created file " + configFile);
  }

  private void info(String message) {
    System.out.println('[' + getClass().getSimpleName() + "] " + message);
  }
}

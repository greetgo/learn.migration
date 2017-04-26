package kz.greetgo.learn.migration.util;

import java.io.File;

public class ConfigFiles {

  private static File configFile(String name) {
    return new File(System.getProperty("user.home") + "/learn.migration.d/" + name);
  }

  public static File operDb() {
    return configFile("oper.db.properties");
  }

  public static File ciaDb() {
    return configFile("migration_source.db.properties");
  }
}

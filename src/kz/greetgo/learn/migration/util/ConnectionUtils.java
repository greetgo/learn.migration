package kz.greetgo.learn.migration.util;

import kz.greetgo.learn.migration.interfaces.ConnectionConfig;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectionUtils {

  public static Connection create(ConnectionConfig connectionConfig) throws Exception {
    Class.forName("org.postgresql.Driver");
    return DriverManager.getConnection(connectionConfig.url(), connectionConfig.user(), connectionConfig.password());
  }

  public static ConnectionConfig fileToConnectionConfig(File configFile) throws IOException {
    ConfigData configData = new ConfigData();
    configData.loadFromFile(configFile);
    return new ConnectionConfig() {
      @Override
      public String url() {
        return configData.str("url");
      }

      @Override
      public String user() {
        return configData.str("user");
      }

      @Override
      public String password() {
        return configData.str("password");
      }
    };
  }
}

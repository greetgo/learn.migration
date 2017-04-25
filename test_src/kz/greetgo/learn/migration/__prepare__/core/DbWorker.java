package kz.greetgo.learn.migration.__prepare__.core;

import kz.greetgo.learn.migration.util.ConfigData;
import kz.greetgo.learn.migration.util.ConfigFiles;
import kz.greetgo.learn.migration.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class DbWorker {

  public void prepareConfigFiles() throws IOException {
    prepareConfigDbFile(ConfigFiles.operDb(), "learn_migration");
    prepareConfigDbFile(ConfigFiles.migrationSourceDb(), "learn_migration_source");
  }

  private void prepareConfigDbFile(File configFile, String db) throws IOException {
    if (configFile.exists()) {
      info("File " + configFile + " already exists");
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("url=").append(DbAdminAccess.changeDb(DbAdminAccess.adminUrl(),
      System.getProperty("user.name") + "_" + db)).append('\n');
    sb.append("user=").append(System.getProperty("user.name")).append('_').append(db).append('\n');
    sb.append("password=7777777\n");

    configFile.getParentFile().mkdirs();
    FileUtils.setStrToFile(sb.toString(), configFile);
    info("Created file " + configFile);
  }

  private void info(String message) {
    System.out.println('[' + getClass().getSimpleName() + "] " + message);
  }

  private Connection createAdminConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    return DriverManager.getConnection(
      DbAdminAccess.adminUrl(), DbAdminAccess.adminUserId(), DbAdminAccess.adminUserPassword());
  }

  private void exec(Connection connection, String sql) {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new ExecFailed(e);
    }

    info("EXECUTED SQL: " + sql);
  }

  public void dropOperDb() throws Exception {
    dropDb(ConfigFiles.operDb());
  }

  private void dropDb(File configFile) throws Exception {
    ConfigData config = new ConfigData();
    config.loadFromFile(configFile);

    String url = config.str("url");
    String dbName = DbAdminAccess.extractDbNameFrom(url);

    try (Connection connection = createAdminConnection()) {

      try {
        exec(connection, "drop database " + dbName);
      } catch (ExecFailed e) {
        info(e.getMessage());
      }

      try {
        exec(connection, "drop user " + config.str("user"));
      } catch (ExecFailed e) {
        info(e.getMessage());
      }
    }
  }

  public void createOperDb() throws Exception {
    createDb(ConfigFiles.operDb());
  }

  private void createDb(File configFile) throws Exception {
    ConfigData config = new ConfigData();
    config.loadFromFile(configFile);

    String url = config.str("url");
    String user = config.str("user");
    String password = config.str("password");
    String dbName = DbAdminAccess.extractDbNameFrom(url);

    try (Connection connection = createAdminConnection()) {
      exec(connection, "create user " + user + " with encrypted password '" + password + "'");
      exec(connection, "create database " + dbName + " with owner " + user);
    }
  }

  public void createMigrationSourceDb() throws Exception {
    createDb(ConfigFiles.migrationSourceDb());
  }

  public void dropMigrationSourceDb() throws Exception {
    dropDb(ConfigFiles.migrationSourceDb());
  }

  public void applyDDL(DDL ddl) throws IOException, SQLException, ClassNotFoundException {
    try (Connection connection = createConnection(ConfigFiles.operDb())) {
      ddl.getDDL().stream()
        .map(FileUtils::fileToStr)
        .flatMap(s -> Arrays.stream(s.split(";;")))
        .map(String::trim)
        .filter(s -> s.length() > 0)
        .forEachOrdered(sql -> exec(connection, sql));
    }
  }

  private Connection createConnection(File configFile) throws IOException, ClassNotFoundException, SQLException {
    ConfigData configData = new ConfigData();
    configData.loadFromFile(configFile);

    String url = configData.str("url");
    String user = configData.str("user");
    String password = configData.str("password");

    Class.forName("org.postgresql.Driver");
    return DriverManager.getConnection(url, user, password);
  }
}

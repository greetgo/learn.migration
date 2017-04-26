package kz.greetgo.learn.migration.core;

import kz.greetgo.learn.migration.interfaces.ConnectionConfig;
import kz.greetgo.learn.migration.util.ConnectionUtils;
import kz.greetgo.learn.migration.util.TimeUtils;
import org.xml.sax.SAXException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Migration implements Closeable {

  private final ConnectionConfig operConfig;


  private final ConnectionConfig ciaConfig;
  private Connection operConnection = null, ciaConnection = null;

  public Migration(ConnectionConfig operConfig, ConnectionConfig ciaConfig) {
    this.operConfig = operConfig;
    this.ciaConfig = ciaConfig;
  }

  @Override
  public void close() {
    closeOperConnection();
    closeCiaConnection();
  }

  private void closeCiaConnection() {
    if (ciaConnection != null) {
      try {
        ciaConnection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      ciaConnection = null;
    }
  }

  private void closeOperConnection() {
    if (this.operConnection != null) {
      try {
        this.operConnection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      this.operConnection = null;
    }
  }

  private void info(String message) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    System.out.println(sdf.format(new Date()) + " [" + getClass().getSimpleName() + "] " + message);
  }

  private String ptr(String sql) {
    sql = sql.replaceAll("TMP_CLIENT", tmpClientTable);
    return sql;
  }

  private void exec(String sql) throws SQLException {
    String executingSql = ptr(sql);

    long startedAt = System.nanoTime();
    try (Statement statement = operConnection.createStatement()) {
      statement.execute(executingSql);
      info("EXECUTE SQL for " + TimeUtils.showTime(System.nanoTime(), startedAt) + " : " + executingSql);
    } catch (SQLException e) {
      info("ERROR EXECUTE SQL for " + TimeUtils.showTime(System.nanoTime(), startedAt)
        + ", message: " + e.getMessage() + ", SQL : " + executingSql);
      throw e;
    }
  }

  public int portionSize = 1000, maxBatchSize = 10;

  private String tmpClientTable;

  public int migrate() throws Exception {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
    Date nowDate = new Date();
    tmpClientTable = "cia_migration_client_" + sdf.format(nowDate);

    operConnection = ConnectionUtils.create(operConfig);

    exec("create table TMP_CLIENT (\n" +
      "  number bigint not null primary key,\n" +
      "  surname varchar(300),\n" +
      "  name varchar(300),\n" +
      "  patronymic varchar(300),\n" +
      "  birth_date date\n" +
      ")");

    ciaConnection = ConnectionUtils.create(ciaConfig);

    int portionSize = download();

    closeCiaConnection();

    return portionSize;
  }

  private int download() throws SQLException, IOException, SAXException {

    try (PreparedStatement ciaPS = ciaConnection.prepareStatement(
      "select * from migration_client where status='JUST_INSERTED' order by number limit ?")) {

      ciaPS.setInt(1, portionSize);

      Insert insert = new Insert("TMP_CLIENT");
      insert.field(1, "number", "?");
      insert.field(2, "cia_id", "?");
      insert.field(3, "surname", "?");
      insert.field(4, "name", "?");
      insert.field(5, "patronymic", "?");
      insert.field(6, "birth_date", "?");

      int batchSize = 0;
      operConnection.setAutoCommit(false);
      try (PreparedStatement operPS = operConnection.prepareStatement(ptr(insert.toString()))) {


        try (ResultSet ciaRS = ciaPS.executeQuery()) {
          while (ciaRS.next()) {
            ClientRecord r = new ClientRecord();
            r.number = ciaRS.getLong("number");
            r.parseRecordData(ciaRS.getString("record_data"));

            operPS.setLong(1, r.number);
            operPS.setString(2, r.id);
            operPS.setString(3, r.surname);
            operPS.setString(4, r.name);
            operPS.setString(5, r.patronymic);
            operPS.setDate(6, r.birthDate);

            operPS.addBatch();
            batchSize++;

            if (batchSize >= maxBatchSize) {
              operPS.executeBatch();
              operConnection.commit();
              batchSize++;
            }

          }

          if (batchSize > 0) {
            operPS.executeBatch();
            operConnection.commit();
          }

        }
      } finally {
        operConnection.setAutoCommit(true);
      }
    }

    return 0;
  }
}

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
import java.util.concurrent.atomic.AtomicBoolean;

import static kz.greetgo.learn.migration.util.TimeUtils.recordsPerSecond;
import static kz.greetgo.learn.migration.util.TimeUtils.showTime;

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

  private String r(String sql) {
    sql = sql.replaceAll("TMP_CLIENT", tmpClientTable);
    return sql;
  }

  private void exec(String sql) throws SQLException {
    String executingSql = r(sql);

    long startedAt = System.nanoTime();
    try (Statement statement = operConnection.createStatement()) {
      statement.execute(executingSql);
      info("EXECUTE SQL for " + showTime(System.nanoTime(), startedAt) + " : " + executingSql);
    } catch (SQLException e) {
      info("ERROR EXECUTE SQL for " + showTime(System.nanoTime(), startedAt)
        + ", message: " + e.getMessage() + ", SQL : " + executingSql);
      throw e;
    }
  }

  public int portionSize = 1_000_000, downloadMaxBatchSize = 50_000;
  public int showStatusPingMillis = 5000;
  public int uploadErrorsMaxBatchSize = downloadMaxBatchSize;

  private String tmpClientTable;

  public int migrate() throws Exception {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
    Date nowDate = new Date();
    tmpClientTable = "cia_migration_client_" + sdf.format(nowDate);

    createOperConnection();

    //language=PostgreSQL
    exec("create table TMP_CLIENT (\n" +
      "  client_id int8,\n" +
      "  status int not null default 0,\n" +
      "  error varchar(300),\n" +
      "  \n" +
      "  number bigint not null primary key,\n" +
      "  cia_id varchar(100) not null,\n" +
      "  surname varchar(300),\n" +
      "  name varchar(300),\n" +
      "  patronymic varchar(300),\n" +
      "  birth_date date\n" +
      ")");

    createCiaConnection();

    int portionSize = download();

    closeCiaConnection();

    innerMigrate();

    return portionSize;
  }

  private void createOperConnection() throws Exception {
    operConnection = ConnectionUtils.create(operConfig);
  }

  private void createCiaConnection() throws Exception {
    ciaConnection = ConnectionUtils.create(ciaConfig);
  }


  private int download() throws SQLException, IOException, SAXException {

    final AtomicBoolean working = new AtomicBoolean(true);
    final AtomicBoolean showStatus = new AtomicBoolean(false);

    final Thread see = new Thread(() -> {

      while (working.get()) {

        try {
          Thread.sleep(showStatusPingMillis);
        } catch (InterruptedException e) {
          break;
        }

        showStatus.set(true);

      }

    });
    see.start();


    try (PreparedStatement ciaPS = ciaConnection.prepareStatement(
      "select * from transition_client where status='JUST_INSERTED' order by number limit ?")) {

      info("Prepared statement for : select * from transition_client");

      ciaPS.setInt(1, portionSize);

      Insert insert = new Insert("TMP_CLIENT");
      insert.field(1, "number", "?");
      insert.field(2, "cia_id", "?");
      insert.field(3, "surname", "?");
      insert.field(4, "name", "?");
      insert.field(5, "patronymic", "?");
      insert.field(6, "birth_date", "?");

      operConnection.setAutoCommit(false);
      try (PreparedStatement operPS = operConnection.prepareStatement(r(insert.toString()))) {

        try (ResultSet ciaRS = ciaPS.executeQuery()) {

          info("Got result set for : select * from transition_client");

          int batchSize = 0, recordsCount = 0;

          long startedAt = System.nanoTime();

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
            recordsCount++;

            if (batchSize >= downloadMaxBatchSize) {
              operPS.executeBatch();
              operConnection.commit();
              batchSize = 0;
            }

            if (showStatus.get()) {
              showStatus.set(false);

              long now = System.nanoTime();
              info(" -- downloaded records " + recordsCount + " for " + showTime(now, startedAt)
                + " : " + recordsPerSecond(recordsCount, now - startedAt));
            }

          }

          if (batchSize > 0) {
            operPS.executeBatch();
            operConnection.commit();
          }

          {
            long now = System.nanoTime();
            info("TOTAL Downloaded records " + recordsCount + " for " + showTime(now, startedAt)
              + " : " + recordsPerSecond(recordsCount, now - startedAt));
          }

          return recordsCount;
        }
      } finally {
        operConnection.setAutoCommit(true);
        working.set(false);
        see.interrupt();
      }
    }
  }


  private void uploadAndDropErrors() throws Exception {
    info("uploadAndDropErrors goes");

    final AtomicBoolean working = new AtomicBoolean(true);

    createCiaConnection();
    ciaConnection.setAutoCommit(false);
    try {

      try (PreparedStatement inPS = operConnection.prepareStatement(r(
        "select number, error from TMP_CLIENT where error is not null"))) {

        info("Prepared statement for : select number, error from TMP_CLIENT where error is not null");

        try (ResultSet inRS = inPS.executeQuery()) {
          info("Query executed for : select number, error from TMP_CLIENT where error is not null");

          try (PreparedStatement outPS = ciaConnection.prepareStatement(
            "update transition_client set status = 'ERROR', error = ? where number = ?")) {

            int batchSize = 0, recordsCount = 0;

            final AtomicBoolean showStatus = new AtomicBoolean(false);

            new Thread(() -> {

              while (working.get()) {

                try {
                  Thread.sleep(showStatusPingMillis);
                } catch (InterruptedException e) {
                  break;
                }

                showStatus.set(true);

              }

            }).start();

            long startedAt = System.nanoTime();

            while (inRS.next()) {

              outPS.setString(1, inRS.getString("error"));
              outPS.setLong(2, inRS.getLong("number"));
              outPS.addBatch();
              batchSize++;
              recordsCount++;

              if (batchSize >= uploadErrorsMaxBatchSize) {
                outPS.executeBatch();
                ciaConnection.commit();
                batchSize = 0;
              }

              if (showStatus.get()) {
                showStatus.set(false);

                long now = System.nanoTime();
                info(" -- uploaded errors " + recordsCount + " for " + TimeUtils.showTime(now, startedAt)
                  + " : " + TimeUtils.recordsPerSecond(recordsCount, now - startedAt));
              }
            }

            if (batchSize > 0) {
              outPS.executeBatch();
              ciaConnection.commit();
            }

            {
              long now = System.nanoTime();
              info("TOTAL Uploaded errors " + recordsCount + " for " + TimeUtils.showTime(now, startedAt)
                + " : " + TimeUtils.recordsPerSecond(recordsCount, now - startedAt));
            }
          }
        }
      }

    } finally {
      closeCiaConnection();
      working.set(false);
    }

    //language=PostgreSQL
    exec("delete from TMP_CLIENT where error is not null");
  }

  private void innerMigrate() throws Exception {

    //language=PostgreSQL
    exec("update TMP_CLIENT set error = 'surname is not defined'\n" +
      "where error is null and surname is null");
    //language=PostgreSQL
    exec("update TMP_CLIENT set error = 'name is not defined'\n" +
      "where error is null and name is null");
    //language=PostgreSQL
    exec("update TMP_CLIENT set error = 'birth_date is not defined'\n" +
      "where error is null and birth_date is null");

    uploadAndDropErrors();

  }
}

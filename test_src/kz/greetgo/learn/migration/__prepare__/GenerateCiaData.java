package kz.greetgo.learn.migration.__prepare__;

import kz.greetgo.learn.migration.__prepare__.core.DbWorker;
import kz.greetgo.learn.migration.util.ConfigFiles;
import kz.greetgo.learn.migration.util.FileUtils;
import kz.greetgo.learn.migration.util.RND;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static kz.greetgo.learn.migration.util.TimeUtils.recordsPerSecond;
import static kz.greetgo.learn.migration.util.TimeUtils.showTime;


public class GenerateCiaData {
  public static void main(String[] args) throws Exception {
    new GenerateCiaData().execute();
  }

  private static final int MAX_STORING_ID_COUNT = 1_000_000;
  private static final int MAX_BATCH_SIZE = 1000;

  void info(String message) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    System.out.println(sdf.format(new Date()) + " [" + getClass().getSimpleName() + "] " + message);
  }

  Connection connection;

  private void execute() throws Exception {
    try (Connection connection = DbWorker.createConnection(ConfigFiles.ciaDb())) {
      this.connection = connection;

      prepareData();

    }
  }

  private final File storingIdsFile = new File("build/storing_ids.txt");
  private final Set<String> storingIdSet = new HashSet<>();
  private List<String> storingIdList = new ArrayList<>();

  private void readStoringIds() {
    if (!storingIdsFile.exists()) return;
    storingIdList = Arrays.stream(FileUtils.fileToStr(storingIdsFile).split("\n"))
      .collect(Collectors.toList());
    storingIdSet.clear();
    storingIdSet.addAll(storingIdList);
  }

  private void saveStoringIds() throws IOException {
    storingIdsFile.getParentFile().mkdirs();
    FileUtils.putStrToFile(storingIdSet.stream().sorted().collect(Collectors.joining("\n")), storingIdsFile);
  }

  String tryAddToStore(String id) {
    if (storingIdSet.size() >= MAX_STORING_ID_COUNT) return id;
    if (storingIdSet.contains(id)) return id;
    storingIdSet.add(id);
    storingIdList.add(id);
    return id;
  }

  String rndStoreId() {
    if (storingIdList.size() == 0) return null;
    return storingIdList.get(RND._int(storingIdList.size()));
  }

  private final File workingFile = new File("build/__working__");

  private void prepareData() throws Exception {

    workingFile.getParentFile().mkdirs();

    final AtomicBoolean working = new AtomicBoolean(true);
    final AtomicBoolean show = new AtomicBoolean(false);

    final Thread see = new Thread(() -> {

      while (workingFile.exists()) {

        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          break;
        }

        show.set(true);

      }

      working.set(false);

    });

    workingFile.createNewFile();
    see.start();

    readStoringIds();

    List<String> newIds = new ArrayList<>();

    connection.setAutoCommit(false);

    try {

      try (PreparedStatement ps = connection.prepareStatement("insert into transition_client (record_data) values (?)")) {

        int batchSize = 0, inserts = 0;

        long startedAt = System.nanoTime();

        while (working.get()) {

          ClientInRecord r = new ClientInRecord();
          r.id = RND.bool(50) ? rndStoreId() : null;
          if (r.id == null) newIds.add(r.id = RND.str(10));
          r.surname = RND.bool(4) ? null : RND.str(20);
          r.name = RND.bool(4) ? null : RND.str(20);
          r.patronymic = RND.bool(10) ? null : RND.str(20);

          r.birthDate = RND.bool(10) ? null : RND.date(-100 * 365, -10 * 365);

          ps.setString(1, r.toXml());
          ps.addBatch();
          batchSize++;
          inserts++;

          if (batchSize >= MAX_BATCH_SIZE) {
            ps.executeBatch();
            connection.commit();
          }

          if (show.get()) {
            show.set(false);
            long now = System.nanoTime();

            info(" -- Inserted records " + inserts + " for "
              + showTime(now, startedAt) + " - " + recordsPerSecond(inserts, now - startedAt));
          }
        }

        if (batchSize > 0) {
          ps.executeBatch();
          connection.commit();
        }

        long now = System.nanoTime();
        info("TOTAL: Inserted records " + inserts + " for "
          + showTime(now, startedAt) + " - " + recordsPerSecond(inserts, now - startedAt));
      }


    } finally {
      connection.setAutoCommit(true);
    }
    newIds.forEach(this::tryAddToStore);

    info("see.join();");
    see.join();

    info("save storing ids...");

    saveStoringIds();

    info("Finish");
  }
}

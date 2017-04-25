package kz.greetgo.learn.migration.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FileUtils {

  public static void setStrToFile(String content, File file) {

    if (content == null) {
      file.delete();
      return;
    }

    try (FileOutputStream fOut = new FileOutputStream(file)) {

      fOut.write(content.getBytes(StandardCharsets.UTF_8));

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}

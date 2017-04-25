package kz.greetgo.learn.migration.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FileUtils {

  public static void setStrToFile(String content, File file) throws IOException {

    if (content == null) {
      file.delete();
      return;
    }

    try (FileOutputStream fOut = new FileOutputStream(file)) {

      fOut.write(content.getBytes(StandardCharsets.UTF_8));

    }
  }

  public static String fileToStr(File file) {
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) file.length());
    try (FileInputStream in = new FileInputStream(file)) {
      byte buffer[] = new byte[4 * 1024];
      while (true) {
        int count = in.read(buffer);
        if (count < 0) return out.toString("UTF-8");
        out.write(buffer, 0, count);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}

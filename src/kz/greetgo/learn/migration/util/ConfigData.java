package kz.greetgo.learn.migration.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class ConfigData {

  private final Map<String, String> data = new HashMap<>();

  public void loadFromFile(File configFile) {
    try {
      loadFromInputStream(new FileInputStream(configFile));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void loadFromInputStream(FileInputStream inputStream) throws IOException {
    try {

      try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
        while (true) {
          String line = br.readLine();
          if (line == null) break;
          if (line.trim().startsWith("#")) continue;

          int idx = line.indexOf('=');
          if (idx < 0) continue;

          String key = line.substring(0, idx).trim();
          String value = line.substring(idx + 1).trim();

          data.put(key, value);
        }
      }

    } finally {
      inputStream.close();
    }
  }

  public String str(String key) {
    if (!data.containsKey(key)) throw new IllegalArgumentException("No key " + key);
    return data.get(key);
  }

}

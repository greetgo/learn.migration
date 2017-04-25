package kz.greetgo.learn.migration.__prepare__.core;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

public class DDLUtil {
  public static List<File> getSortedSqlFiles(Class<?> aClass) {
    URL url = aClass.getResource(".");
    
    File[] files = new File(url.getFile())
      .listFiles(pn -> pn.getName().toLowerCase().endsWith(".sql"));

    return Arrays.asList(files != null ? files : new File[0]);
  }
}

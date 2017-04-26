package kz.greetgo.learn.migration.__prepare__.db.cia;

import kz.greetgo.learn.migration.__prepare__.core.DDL;
import kz.greetgo.learn.migration.__prepare__.core.DDLUtil;

import java.io.File;
import java.util.List;

public class CiaDDL implements DDL {

  private CiaDDL() {
  }

  private enum StoringEnum {
    STORING_ELEMENT;
    CiaDDL value;

    CiaDDL getValue() {
      if (this.value == null) {
        synchronized (this) {
          if (this.value == null) {
            return this.value = new CiaDDL();
          }
        }
      }
      return this.value;
    }
  }

  public static CiaDDL get() {
    return StoringEnum.STORING_ELEMENT.getValue();
  }

  @Override
  public List<File> getDDL() {
    return DDLUtil.getSortedSqlFiles(getClass());
  }
}

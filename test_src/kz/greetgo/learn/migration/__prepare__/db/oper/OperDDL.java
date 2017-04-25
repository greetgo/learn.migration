package kz.greetgo.learn.migration.__prepare__.db.oper;

import kz.greetgo.learn.migration.__prepare__.core.DDL;
import kz.greetgo.learn.migration.__prepare__.core.DDLUtil;

import java.io.File;
import java.util.List;

public class OperDDL implements DDL {

  private OperDDL() {
  }

  private enum StoringEnum {
    STORING_ELEMENT;
    OperDDL value;

    OperDDL getValue() {
      if (this.value == null) {
        synchronized (this) {
          if (this.value == null) {
            return this.value = new OperDDL();
          }
        }
      }
      return this.value;
    }
  }

  public static OperDDL get() {
    return StoringEnum.STORING_ELEMENT.getValue();
  }

  @Override
  public List<File> getDDL() {
    return DDLUtil.getSortedSqlFiles(getClass());
  }
}

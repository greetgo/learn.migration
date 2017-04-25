package kz.greetgo.learn.migration.__prepare__.core;

import java.sql.SQLException;

public class ExecFailed extends RuntimeException {
  public ExecFailed(SQLException e) {
    super(e.getMessage(), e);
  }
}

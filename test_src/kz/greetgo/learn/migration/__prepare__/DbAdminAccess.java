package kz.greetgo.learn.migration.__prepare__;

public class DbAdminAccess {

  public static String adminUrl() {
    return envOrDefault("PG_ADMIN_URL", "jdbc:postgresql://localhost/postgres");
  }

  public static String adminUserId() {
    return envOrDefault("PG_ADMIN_USERID", "postgres");
  }

  public static String adminUserPassword() {
    return envOrDefault("PG_ADMIN_PASSWORD", "very very big secret");
  }

  public static String changeDb(String url, String db) {
    int idx = url.lastIndexOf('/');
    if (idx < 0) throw new IllegalArgumentException("Cannot change db in url = " + url);
    return url.substring(0, idx + 1) + db;
  }

  private static String envOrDefault(String envKey, String defaultValue) {
    String value = System.getenv(envKey);
    if (value == null) return defaultValue;
    value = value.trim();
    if (value.length() == 0) return defaultValue;
    return value;
  }

  public static void main(String[] args) {
    System.out.println("adminUrl = " + adminUrl());
    System.out.println("adminUserId = " + adminUserId());
    System.out.println("adminUserPassword = " + adminUserPassword());
  }
}

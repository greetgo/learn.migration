package kz.greetgo.learn.migration.__prepare__.core;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ClientInRecord {
  public String id;
  public String surname, name, patronymic;
  public Date birthDate;

  public String toXml() {
    StringBuilder sb = new StringBuilder();
    sb.append("<record id=\"").append(id).append("\">\n");
    sb.append("  <names");
    if (surname != null) sb.append(" surname=\"").append(surname).append("\"");
    if (name != null) sb.append(" name=\"").append(name).append("\"");
    if (patronymic != null) sb.append(" patronymic=\"").append(patronymic).append("\"");
    sb.append("/>\n");

    if (birthDate != null) {
      SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
      sb.append("  <birth_date>").append(sdf.format(birthDate)).append("</birth_date>\n");
    }

    sb.append("</record>\n");
    return sb.toString();
  }

  public static void main(String[] args) {
    ClientInRecord r = new ClientInRecord();
    r.id = "ID";
    r.surname = "Surname";
    r.name = "Pat";
    r.patronymic = "Pat";
    r.birthDate = new Date();

    System.out.println(r.toXml());
  }
}

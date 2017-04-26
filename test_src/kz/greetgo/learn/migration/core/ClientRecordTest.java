package kz.greetgo.learn.migration.core;

import kz.greetgo.learn.migration.__prepare__.ClientInRecord;
import kz.greetgo.learn.migration.util.RND;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class ClientRecordTest {

  @Test
  public void parseRecordData() throws Exception {
    ClientInRecord in = new ClientInRecord();
    in.id = RND.str(10);
    in.surname = RND.str(10);
    in.name = RND.str(10);
    in.patronymic = RND.str(10);
    in.birthDate = RND.date(-10000, -10);

    ClientRecord record = new ClientRecord();
    record.parseRecordData(in.toXml());

    assertThat(record.id).isEqualTo(in.id);
    assertThat(record.surname).isEqualTo(in.surname);
    assertThat(record.name).isEqualTo(in.name);
    assertThat(record.patronymic).isEqualTo(in.patronymic);
    assertThat(record.birthDate).isEqualTo(in.birthDate);
  }
}
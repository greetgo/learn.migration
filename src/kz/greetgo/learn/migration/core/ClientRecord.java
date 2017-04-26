package kz.greetgo.learn.migration.core;

import kz.greetgo.learn.migration.util.SaxHandler;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;

public class ClientRecord extends SaxHandler {
  public long number;
  public String id;
  public String surname, name, patronymic;
  public java.sql.Date birthDate;

  public void parseRecordData(String recordData) throws SAXException, IOException {
    if (recordData == null) return;
    XMLReader reader = XMLReaderFactory.createXMLReader();
    reader.setContentHandler(this);
    reader.parse(new InputSource(new StringReader(recordData)));
  }


  @Override
  protected void startingTag(Attributes attributes) {
    String path = path();
    if ("/record".equals(path)) {
      id = attributes.getValue("id");
      return;
    }
    if ("/record/names".equals(path)) {
      surname = attributes.getValue("surname");
      name = attributes.getValue("name");
      patronymic = attributes.getValue("patronymic");
      return;
    }
  }

  @Override
  protected void endedTag(String tagName) throws Exception {

    String path = path() + "/" + tagName;

    if ("/record/birth_date".equals(path)) {
      SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
      birthDate = new java.sql.Date(sdf.parse(text()).getTime());
      return;
    }
  }
}

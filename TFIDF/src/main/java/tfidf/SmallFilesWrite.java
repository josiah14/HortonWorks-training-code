package tfidf;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class SmallFilesWrite {

  public static final String FIELD_FILENAME = "filename";
  public static final String FIELD_CONTENTS = "contents";
  private static final String SCHEMA_JSON = 
          "{\"type\": \"record\", \"name\": \"SmallFiles\", "
          + "\"fields\": ["
          + "{\"name\":\"" + FIELD_FILENAME
          + "\", \"type\":\"string\"},"
          + "{\"name\":\"" + FIELD_CONTENTS
          + "\", \"type\":\"bytes\"}]}";
  public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

  public static void writeToAvro(File srcPath,
          OutputStream outputStream)
          throws IOException {
    Collection<File> files = FileUtils.listFiles(srcPath, null, false);

    @SuppressWarnings("resource")
    DataFileWriter<Object> writer =
            new DataFileWriter<Object>(
                new GenericDatumWriter<Object>())
                .setSyncInterval(100);                 
    writer.setCodec(CodecFactory.deflateCodec(5));  
    writer.setMeta("recordCount", files.size());
    writer.create(SCHEMA, outputStream);
    for (Object obj : files) {
      File file = (File) obj;
      String filename = file.getName();
      byte content[] = FileUtils.readFileToByteArray(file);
      GenericRecord record = new GenericData.Record(SCHEMA);  
      record.put(FIELD_FILENAME, new Utf8(filename));                  
      record.put(FIELD_CONTENTS, ByteBuffer.wrap(content)); 
      writer.append(record);                                 
    }
    
    System.out.println("Wrote " + files.size() + " records");

    IOUtils.cleanup(null, writer);
    IOUtils.cleanup(null, outputStream);
  }

  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);

    File sourceDir = new File(args[0]);
    Path destFile = new Path(args[1]);

    OutputStream os = hdfs.create(destFile);
    writeToAvro(sourceDir, os);
  }
}
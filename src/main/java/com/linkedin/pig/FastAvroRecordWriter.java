package com.linkedin.pig;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;

public class FastAvroRecordWriter extends RecordWriter<NullWritable, Object> {

  Schema schema;
  DataFileWriter<GenericData.Record> writer;
  Path out;
  
  FastAvroRecordWriter(Schema s, Path o, Configuration c) throws IOException {
    schema = s;
    out = o;
    DatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<GenericData.Record>(s);
    writer = new DataFileWriter<GenericData.Record>(datumWriter);
    writer.create(s, out.getFileSystem(c).create(out));
  }
  
  @Override
  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
    writer.close();
  }

  @Override
  public void write(NullWritable key, Object value) throws IOException, InterruptedException {

    if (value instanceof GenericData.Record) {
      // whoo-hoo! already avro
      writer.append( (GenericData.Record) value );
    } else if (value instanceof Tuple) {
      // pack the damn thing into an Avro record
      writer.append(FastAvroStorageCommon.packIntoAvro((Tuple) value, schema));
    }
  }
  
}

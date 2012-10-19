/**
 * 
 */
package com.linkedin.pig;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * @author jadler
 *
 */
public class FastAvroRecordReader 
  extends RecordReader<NullWritable, GenericData.Record> {

  private FileReader<GenericData.Record> reader;
  private long start;
  private long end;
  private Schema schema;
  private GenericData.Record currentRecord;

  FastAvroRecordReader(Schema s) {
    schema = s;
  }
  
  @Override
  public void close() throws IOException {
    reader.close();
  }
  
  @Override
  public GenericData.Record getCurrentValue() throws IOException, InterruptedException {
    if (currentRecord != null)
      return currentRecord;
    else
      return null;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, ((float)(reader.tell() - start)) / ((float)(end - start)));
    }
  }

  @Override
  public void initialize(InputSplit isplit, TaskAttemptContext tc) throws IOException, InterruptedException {

    FileSplit fsplit = (FileSplit) isplit;
    start  = fsplit.getStart();
    end    = fsplit.getStart() + fsplit.getLength();    
    reader = DataFileReader.openReader
       (new FsInput(fsplit.getPath(), tc.getConfiguration()),
           new GenericDatumReader<GenericData.Record>(schema));
    reader.sync(start);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    if (reader.pastSync(end))
      return false;
    try {
      currentRecord = reader.next();
    } catch (NoSuchElementException e) {
      return false;
    }
    return true;
  }

}

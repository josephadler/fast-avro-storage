/**
 * 
 */
package com.linkedin.pig;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.collect.Maps;

/**
 * @author Joseph adler
 * Pig Storage function for reading and writing Avro data.
 * 
 */
public class FastAvroStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata {

  /**
   *  Creates new instance of Pig Storage function, without specifying the output schema. Useful
   *  for just loading in data.
   */  
  public FastAvroStorage() {
  }

  private String schemaName = "please_change_me";
  private String schemaNameSpace = null;
  protected Schema schema;

  /**
   *  Creates new instance of Pig Storage function, specifying output schema properties.
   *  @param vars Input value array. First item in the array may be an avro schema or 
   *  may just be the name of the output record type. Second item in the array may be a 
   *  namespace for the output schema. Arguments after the second are currently silently
   *  ignored.
   *  
   */  
  public FastAvroStorage(String[] vars) {
    if (vars != null && vars.length > 0) {
      try {
        schema = (new Schema.Parser()).parse(vars[0]);
      } catch (Exception e) {
        schemaName = vars[0];
      }
    }
    if (vars != null && vars.length > 1) {
      schemaNameSpace = vars[1];
    }
  }

  private String UDFContextSignature = null;

  @Override
  public void setUDFContextSignature(String signature) {
    UDFContextSignature = signature;
    super.setUDFContextSignature(signature);
  }

  protected Properties getProperties() {
    if (UDFContextSignature == null)
      return getProperties(this.getClass(), null);
    else
      return getProperties(this.getClass(), UDFContextSignature);
  }

  @SuppressWarnings("rawtypes")
  protected Properties getProperties(Class c, String signature) {
    UDFContext context = UDFContext.getUDFContext();
    if (signature == null)
      return context.getUDFProperties(c);
    else
      return context.getUDFProperties(c, new String[] { signature });

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadMetadata#getSchema(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    Schema s = getAvroSchema(location, job);
    schema = s;
    return FastAvroStorageCommon.avroSchemaToResourceSchema(s);
  }

  public  Schema getAvroSchema(String location, Job job) throws IOException {
    return getAvroSchema(new Path(location), job);
  }

  
  public static final PathFilter visibleFiles = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return (!p.getName().startsWith("_"));
    }
  };

  public static final PathFilter visibleAvroFiles = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return (( ! p.getName().startsWith("_")) &&
          p.getName().endsWith(org.apache.avro.mapred.AvroOutputFormat.EXT));
    }
  };

  
  public  Schema getAvroSchema(Path p, Job job) throws IOException {
    GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
    FileSystem fs = FileSystem.get(job.getConfiguration());
    FileStatus[] statusArray = fs.globStatus(p, visibleFiles);
    if (statusArray == null)
      throw new IOException("Path " + p.toString() + " does not exist.");
    if (statusArray.length == 0)
      throw new IOException("No path matches pattern " + p.toString());
    p = statusArray[0].getPath();
    while (!fs.isFile(p)) {
      if (fs.getFileStatus(p).isDir())
        statusArray = fs.listStatus(p, visibleFiles);
      if (statusArray.length == 0)
        throw new IOException("not files match path " + p);
      else
        p = statusArray[0].getPath();
    }
    InputStream hdfsInputStream = fs.open(p);
    DataFileStream<Object> avroDataStream = new DataFileStream<Object>(hdfsInputStream, avroReader);
    Schema s = avroDataStream.getSchema();
    avroDataStream.close();
    return s;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadMetadata#getStatistics(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadMetadata#getPartitionKeys(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.pig.LoadMetadata#setPartitionFilter(org.apache.pig.Expression)
   */
  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.pig.StoreFuncInterface#relToAbsPathForStoreLocation(java.lang
   * .String, org.apache.hadoop.fs.Path)
   */
  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
    return LoadFunc.getAbsolutePath(location, curDir);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
   */
  @Override
  public OutputFormat<NullWritable, Object> getOutputFormat() throws IOException {

    class FastAvroStorageOutputFormat extends
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<NullWritable, Object> {

      private Schema schema;

      FastAvroStorageOutputFormat(Schema s) {
        schema = s;
        if (s == null) {
          String schemaString = getProperties().getProperty(OUTPUT_AVRO_SCHEMA);
          if (schemaString != null)
            schema = (new Schema.Parser()).parse(schemaString);
        }

      }

      @Override
      public RecordWriter<NullWritable, Object> getRecordWriter(TaskAttemptContext tc) throws IOException,
          InterruptedException {

        if (schema == null) {
          String schemaString = getProperties(ResourceSchema.class, null).getProperty(OUTPUT_AVRO_SCHEMA);
          if (schemaString != null)
            schema = (new Schema.Parser()).parse(schemaString);
        }

        RecordWriter<NullWritable, Object> rw = new FastAvroRecordWriter(schema, getDefaultWorkFile(tc, ".avro"),
            tc.getConfiguration());

        return rw;
      }
    }

    return new FastAvroStorageOutputFormat(schema);

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.StoreFuncInterface#setStoreLocation(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.pig.StoreFuncInterface#checkSchema(org.apache.pig.ResourceSchema
   * )
   */

  public static final String OUTPUT_AVRO_SCHEMA = "com.linkedin.pig.FastAvroStorage.output.schema";

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    schema = FastAvroStorageCommon.resourceSchemaToAvroSchema(s, schemaName, schemaNameSpace,
        Maps.<String, List<Schema>> newHashMap());

    getProperties(ResourceSchema.class, null).setProperty(OUTPUT_AVRO_SCHEMA, schema.toString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.pig.StoreFuncInterface#prepareToWrite(org.apache.hadoop.mapreduce
   * .RecordWriter)
   */

  private RecordWriter<NullWritable, Object> writer;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void prepareToWrite(RecordWriter w) throws IOException {
    writer = (RecordWriter<NullWritable, Object>) w;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.StoreFuncInterface#putNext(org.apache.pig.data.Tuple)
   */
  @Override
  public void putNext(Tuple t) throws IOException {
    try {
      writer.write(null, t);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.pig.StoreFuncInterface#setStoreFuncUDFContextSignature(java.
   * lang.String)
   */
  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    UDFContextSignature = signature;
    super.setUDFContextSignature(signature);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.StoreFuncInterface#cleanupOnFailure(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    StoreFunc.cleanupOnFailureImpl(location, job);
  }

  public static String INPUT_AVRO_SCHEMA = "INPUT_AVRO_SCHEMA";

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadFunc#setLocation(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
    if (schema == null) {
      schema = getInputAvroSchema();
      if (schema == null) {
        schema = getAvroSchema(location, job);
        if (schema == null)
          throw new IOException("Could not determine avro schema for location " + location);
        setInputAvroSchema(schema);
      }
    }
  }

  protected void setInputAvroSchema(Schema s) {
    getProperties().setProperty(INPUT_AVRO_SCHEMA, s.toString());
  }

  public Schema getInputAvroSchema() {
    String schemaString = getProperties().getProperty(INPUT_AVRO_SCHEMA);
    if (schemaString == null)
      return null;
    schema = new Schema.Parser().parse(schemaString);
    return schema;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadFunc#getInputFormat()
   */
  @Override
  public InputFormat<NullWritable, GenericData.Record> getInputFormat() throws IOException {

    return new PigFileInputFormat<NullWritable, GenericData.Record> () {

      @Override
      public RecordReader<NullWritable, GenericData.Record> createRecordReader(InputSplit is, TaskAttemptContext tc)
          throws IOException, InterruptedException {
        RecordReader<NullWritable, GenericData.Record> rr = new FastAvroRecordReader(getInputAvroSchema());
        rr.initialize(is, tc);
        tc.setStatus(is.toString());
        return rr;
      }
    };

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.pig.LoadFunc#prepareToRead(org.apache.hadoop.mapreduce.RecordReader
   * , org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit)
   */

  @SuppressWarnings("rawtypes")
  RecordReader reader;
  PigSplit split;

  @Override
  public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader r, PigSplit s) throws IOException {
    reader = r;
    split = s;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadFunc#getNext()
   */

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (reader.nextKeyValue())
        return new TupleAvroWrapper<GenericData.Record>((GenericData.Record) reader.getCurrentValue());
      else
        return null;
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new IOException("Wrapped Interrupted Exception" + e);
    }
  }
}

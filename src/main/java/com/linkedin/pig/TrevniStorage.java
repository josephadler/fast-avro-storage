package com.linkedin.pig;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Formatter;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnWriter;
import org.apache.trevni.avro.AvroTrevniOutputFormat;
import org.apache.trevni.avro.HadoopInput;

import com.google.common.collect.Lists;

/**
 * @author jadler@linkedin.com
 *
 * Pig Store/Load Function for Trevni
 *
 */

public class TrevniStorage extends FastAvroStorage implements LoadPushDown{

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadPushDown#getFeatures()
   */

  public TrevniStorage() {    
  }
  
  public TrevniStorage(String[] vars) {
    super(vars);
  }
  
  public static final PathFilter visibleTrevniFiles = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return (!p.getName().startsWith("_") &&
          p.getName().endsWith(org.apache.trevni.avro.AvroTrevniOutputFormat.EXT));
    }
  };
  
  
  @Override
  public List<OperatorSet> getFeatures() {
    return Lists.newArrayList(LoadPushDown.OperatorSet.PROJECTION);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.pig.LoadPushDown#pushProjection(org.apache.pig.LoadPushDown.
   * RequiredFieldList)
   */

  RequiredFieldList requiredFieldList;

  private void requiredFieldListPrinter(List<RequiredField> rfl, int level) {
    for (RequiredField rf: rfl) {
      for (int i=0; i <=level; i++)
        System.err.print("\t");
      System.err.printf("%s\n",rf.getAlias());
      if (rf.getSubFields() != null)
        requiredFieldListPrinter(rf.getSubFields(),level+1);
    }
  }
  
  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList rfl) throws FrontendException {
    requiredFieldList = rfl;
    System.err.printf("Requested fields:\n");
    requiredFieldListPrinter(rfl.getFields(), 0);
    
    Schema newSchema = FastAvroStorageCommon.newSchemaFromRequiredFieldList(schema, rfl);
    if (newSchema != null) {
      schema = newSchema;
      setInputAvroSchema(schema);
      System.err.printf("Only grabbing subset %s\n", getInputAvroSchema());
      return new RequiredFieldResponse(true);
    } else{
      System.err.printf("could not select fields subset %s\n", rfl);
      return new RequiredFieldResponse(false);
    }
  }
    
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadFunc#getInputFormat()
   */
  @Override
  public InputFormat<NullWritable, GenericData.Record> getInputFormat() throws IOException {

      class TrevniStorageInputFormat extends PigFileInputFormat<NullWritable,GenericData.Record> {

        //TrevniStorageInputFormat() {}
        
        @Override protected boolean isSplitable(JobContext jc, Path p) {
          return false;
        }
        
        @Override protected  List<FileStatus> listStatus(JobContext job) throws IOException {
          List<FileStatus> results = Lists.newArrayList();
          job.getConfiguration().setBoolean("mapred.input.dir.recursive", true);          
          for (FileStatus file : super.listStatus(job))
            if (visibleTrevniFiles.accept(file.getPath()))
              results.add(file);
          return results;
        }
        
        @Override
        public RecordReader<NullWritable, GenericData.Record> createRecordReader(InputSplit is, TaskAttemptContext tc)
            throws IOException, InterruptedException {          
          RecordReader<NullWritable, GenericData.Record> rr =  
              new RecordReader<NullWritable, GenericData.Record>() {

            private FileSplit fsplit;
            private AvroColumnReader.Params params;
            private AvroColumnReader<GenericData.Record> reader;
            private float rows;
            private long row = 0;
            private GenericData.Record currentRecord = null;
            
            @Override
            public void close() throws IOException {
              reader.close();
            }

            @Override
            public NullWritable getCurrentKey() throws IOException, InterruptedException {
              return NullWritable.get();
            }

            @Override
            public Record getCurrentValue() throws IOException, InterruptedException {
              return currentRecord;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
              return row / rows;
            }

            @Override
            public void initialize(InputSplit isplit, TaskAttemptContext tac) throws IOException, InterruptedException {
              // TODO Auto-generated method stub
              fsplit = (FileSplit) isplit;
              params = new AvroColumnReader.Params(
                  new HadoopInput(fsplit.getPath(), tac.getConfiguration()));
              Schema inputSchema = getInputAvroSchema();
              System.err.printf("initializing RecordReader with schema %s\n", inputSchema);
              params.setSchema(inputSchema);
              reader = new AvroColumnReader<GenericData.Record>(params);
              rows = reader.getRowCount();
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
              if (reader.hasNext()) {
                currentRecord = reader.next();
                row++;
                return true;
              } else {
                return false;
              }
                
            }};
            
            rr.initialize(is, tc);
            tc.setStatus(is.toString());
            return rr;
        }
        
      }
      
      return new TrevniStorageInputFormat();
  
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
   */
  @Override
  public OutputFormat<NullWritable, Object> getOutputFormat() throws IOException {
    class TrevniStorageOutputFormat extends
      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<NullWritable, Object> {

      private Schema schema;

      TrevniStorageOutputFormat(Schema s) {
        schema = s;
        if (s == null) {
          String schemaString = getProperties().getProperty(OUTPUT_AVRO_SCHEMA);
          if (schemaString != null)
            schema = (new Schema.Parser()).parse(schemaString);
        }

      }

      @Override
      public RecordWriter<NullWritable, Object> getRecordWriter(final TaskAttemptContext tc) throws IOException,
      InterruptedException {

        if (schema == null) {
          String schemaString = getProperties(ResourceSchema.class, null).getProperty(OUTPUT_AVRO_SCHEMA);
          if (schemaString != null)
            schema = (new Schema.Parser()).parse(schemaString);
        }

        final ColumnFileMetaData meta = new ColumnFileMetaData();
        final Path dir = getOutputPath(tc);
        final FileSystem fs = FileSystem.get(tc.getConfiguration());
        final long blockSize = fs.getDefaultBlockSize();
        
        if (!fs.mkdirs(dir))
          throw new IOException("Failed to create directory: " + dir);

        meta.setCodec("snappy");
        
        return new RecordWriter<NullWritable, Object>() {
          private int part = 0;
          private AvroColumnWriter<GenericData.Record> writer =
              new AvroColumnWriter<GenericData.Record>(schema, meta);

          private void flush() throws IOException {
            Integer taskAttemptId = tc.getTaskAttemptID().getTaskID().getId();
            String partName = String.format("%05d_%03d", taskAttemptId, part++);
            OutputStream out = fs.create(new Path(dir, "part-"+partName+AvroTrevniOutputFormat.EXT));
            try {
              writer.writeTo(out);
            } finally {
              out.close();
            }
            writer = new AvroColumnWriter<GenericData.Record>(schema, meta);
          }
          
          @Override
          public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            flush();
          }

          @Override
          public void write(NullWritable n, Object o) throws IOException, InterruptedException {
            GenericData.Record r = FastAvroStorageCommon.packIntoAvro((Tuple)o,schema);
            writer.write(r);
            if (writer.sizeEstimate() >= blockSize)
              flush();
          }
          
        };        
      }
    }

    return new TrevniStorageOutputFormat(schema);
  }
  
  @Override
  public  Schema getAvroSchema(Path p, Job job) throws IOException {

    FileSystem fs = FileSystem.get(job.getConfiguration());

    if (!fs.exists(p))
      throw new IOException("Path " + p + " does not exist");    
    
    while (fs.getFileStatus(p).isDir()) {
      FileStatus statusArray[] = fs.listStatus(p, visibleFiles);
      if (statusArray != null && statusArray.length > 0)
        p = statusArray[0].getPath();
    }

    if (visibleTrevniFiles.accept(p)) {
      
      AvroColumnReader.Params params =
          new AvroColumnReader.Params(new HadoopInput(p, job.getConfiguration()));
       AvroColumnReader<GenericData.Record> reader = 
          new AvroColumnReader<GenericData.Record>(params);
       return reader.getFileSchema();
    } else
      throw new IOException("overly simple file search didn't find a trevni file");
  }
  
}

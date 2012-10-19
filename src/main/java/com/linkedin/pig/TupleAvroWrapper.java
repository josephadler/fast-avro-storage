package com.linkedin.pig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
//import org.apache.avro.io.Decoder;
//import org.apache.avro.io.Encoder;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.google.common.collect.Lists;

public class TupleAvroWrapper <T extends IndexedRecord>  implements Tuple {
  
  T avroObject;
  
  public TupleAvroWrapper(T o) {
    avroObject = o;
  }

  // private Encoder encoder;
  
  @Override
  public void write(DataOutput o) throws IOException {
    throw new IOException("write called, but not supported yet");
    /*
    final class DataOutputToOutputStream extends java.io.OutputStream {
      private DataOutput d;    
      DataOutputToOutputStream(DataOutput out) { d = out; }
      @Override public void write(int b) throws IOException { d.write(b);}    
    }
    
    encoder = EncoderFactory.get().blockingBinaryEncoder(
        new DataOutputToOutputStream(o), null);
    DatumWriter<T> writer = new GenericDatumWriter<T>(avroObject.getSchema());
    writer.write(avroObject, encoder);    
    */
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compareTo(Object o) {
    if (o instanceof TupleAvroWrapper) {
      return GenericData.get().compare(avroObject, 
          ((TupleAvroWrapper) o).avroObject, 
          avroObject.getSchema());
    }   
    return -1;
  }

  @Override
  public void append(Object o) {
    List<Field> fields = avroObject.getSchema().getFields();
    avroObject.put(fields.size(),o);
    Schema fieldSchema = null;
    if (o instanceof String)
      fieldSchema = Schema.create(Type.STRING);
    else if (o instanceof Integer)
      fieldSchema = Schema.create(Type.INT);
    else if (o instanceof Long)
      fieldSchema = Schema.create(Type.LONG);
    else if (o instanceof Double)
      fieldSchema = Schema.create(Type.DOUBLE);
    else if (o instanceof Float)
      fieldSchema = Schema.create(Type.FLOAT);
    else if (o == null)
      fieldSchema = Schema.create(Type.NULL);
    else if (o instanceof Boolean)
      fieldSchema = Schema.create(Type.BOOLEAN);
    else if (o instanceof Map)
      fieldSchema = Schema.create(Type.MAP);
    Field newField = new Field("FIELD_" + fields.size(),fieldSchema,"",null);    
    fields.add(newField);
    avroObject.getSchema().setFields(fields);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object get(int pos) throws ExecException {

    Schema s = avroObject.getSchema().getFields().get(pos).schema();
    Object o = avroObject.get(pos);

    switch(s.getType()) {
      case STRING:
        // damn UTF8 encoding gets in the way here
        return o.toString();
      case MAP:
        return new MapAvroWrapper((Map<CharSequence, Object>)o);
      case RECORD:
        return new TupleAvroWrapper<T>((T) o);
      case ENUM:
        return o.toString();
      case ARRAY:
        return new BagAvroWrapper<GenericData.Record>((GenericArray<GenericData.Record>) o);
      case FIXED:
        return new DataByteArray(((GenericData.Fixed)o).bytes());
      case BYTES:
        return new DataByteArray(((ByteBuffer)o).array());
      case UNION:
        if (o instanceof org.apache.avro.util.Utf8 ) {
          return o.toString();
        } if (o instanceof IndexedRecord) {
          return new TupleAvroWrapper<T>((T) o);
        } if (o instanceof GenericArray) {
          return new BagAvroWrapper<GenericData.Record>((GenericArray<GenericData.Record>) o);
        } if (o instanceof Map) {
          return new MapAvroWrapper((Map<CharSequence, Object>)o);
        } if (o instanceof GenericData.Fixed) {
          return new DataByteArray(((GenericData.Fixed)o).bytes());
        } if (o instanceof ByteBuffer) {
          return new DataByteArray(((ByteBuffer)o).array());
        }
      default:
        return o;
    }
    
  }

  static int counter = 0;
  
  @Override
  public List<Object> getAll() {
    
    List<Object> all = Lists.newArrayList();
    for (Schema.Field f : avroObject.getSchema().getFields()) {
      try {
        all.add(get(f.pos()));
      } catch (ExecException e) {
        System.err.printf("could not process tuple with contents %s\n",avroObject);
        e.printStackTrace();
        return null;
      }
    }
    return all;
  }

  @Override
  public long getMemorySize() {
    return getMemorySize(avroObject);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private long getMemorySize(IndexedRecord r) {
    int total = 0;
    for (Field f : r.getSchema().getFields()){
      switch (f.schema().getType()) {
      case BOOLEAN:
      case ENUM:
      case INT:
        total += Integer.SIZE << 8;
        break;
      case DOUBLE:
        total += Double.SIZE << 8;
        break;
      case FLOAT:
        total += Float.SIZE << 8;
        break;
      case NULL:
        break;
      case STRING:
        total += ((String) r.get(f.pos())).length() * (Character.SIZE << 8);
        break;
      case BYTES:
        total += ((Byte[]) r.get(f.pos())).length;
        break;
      case RECORD:
        total += new TupleAvroWrapper((IndexedRecord) r.get(f.pos())).getMemorySize();
        break;
      case ARRAY:
        total += new BagAvroWrapper((GenericArray) r.get(f.pos())).getMemorySize();
        break;
      }      
    }
    return total;
  }


  
  @Override
  public byte getType(int arg0) throws ExecException {
    Schema s = avroObject.getSchema().getFields().get(arg0).schema(); 
    return FastAvroStorageCommon.getPigType(s);
  }

  @Override
  public boolean isNull() {
    return avroObject == null;
  }

  @Override
  public boolean isNull(int arg0) throws ExecException {
    return avroObject == null || avroObject.get(arg0) == null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void reference(Tuple arg0) {
    avroObject = (T) ((TupleAvroWrapper<T>) arg0).avroObject;
  }

  @Override
  public void set(int arg0, Object arg1) throws ExecException {
    avroObject.put(arg0, arg1);
  }

  @Override
  public void setNull(boolean arg0) {
    if (arg0)
      avroObject = null;
  }

  @Override
  public int size() {
    return avroObject.getSchema().getFields().size();
  }

  @Override
  public String toDelimitedString(String arg0) throws ExecException {
    StringBuffer delimitedString = new StringBuffer();
    boolean notfirst = false;
    for (Field f : avroObject.getSchema().getFields()) {
      if (notfirst) {
        delimitedString.append(arg0);
        notfirst = true;
      }
      delimitedString.append(avroObject.get(f.pos()).toString());
    }
    return delimitedString.toString();
  }
  
  //private Decoder decoder = null;
  
  @Override
  public void readFields(DataInput d) throws IOException {

    throw new IOException("readFields calledn but not supported yet");
    /*
    final class DataInputToInputStream extends java.io.InputStream {
      private DataInput d;    
      DataInputToInputStream(DataInput in) { d = in; }    
      @Override public int read() throws IOException { return d.readByte();}    
    }

    decoder = DecoderFactory.get().binaryDecoder(new DataInputToInputStream(d), null);
    DatumReader<T> reader = new GenericDatumReader<T>(avroObject.getSchema());
    reader.read(avroObject, decoder);
    */
  }

}

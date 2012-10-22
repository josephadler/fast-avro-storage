/**
 * 
 */
package com.linkedin.pig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.ResourceSchema;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author jadler
 *
 * Static methods for working with Pig and Avro data
 * 
 */
public class FastAvroStorageCommon {

  /**
   * Determines the pig object type of the Avro schema
   * @param s The avro schema for which to determine the type
   * @return the byte representing the schema type
   * @see org.apache.avro.Schema.Type
   */  
  public static byte getPigType(Schema s) throws ExecException {
    switch(s.getType()) {
    case ARRAY:
      return DataType.BAG;
    case BOOLEAN:
      return DataType.BOOLEAN;
    case BYTES:
      return DataType.BYTEARRAY;
    case DOUBLE:
      return DataType.DOUBLE;
    case ENUM:
      return DataType.CHARARRAY;
    case FIXED:
      return DataType.BYTEARRAY;
    case FLOAT:
      return DataType.FLOAT;
    case INT:
      return DataType.INTEGER;
    case LONG:
      return DataType.LONG;
    case MAP:
      return DataType.MAP;
    case NULL:
      return DataType.NULL;
    case RECORD:
      return DataType.TUPLE;
    case STRING:
      return DataType.CHARARRAY;
    case UNION:
      List<Schema> types = s.getTypes();
      if (types.size() == 2) {
        if (types.get(0).getType() == Type.NULL)
          return getPigType(types.get(1));
        else if (types.get(1).getType() == Type.NULL)
          return getPigType(types.get(0));
      }
      throw new ExecException("Currently only supports two element unions of a type and null");
    default:
      throw new ExecException("Unknown type");
    }
  }

  /**
   * Translates an Avro schema to a Resource Schema (for Pig)
   * @param s The avro schema for which to determine the type
   * @return the corresponding pig schema
   */  
  public static ResourceSchema avroSchemaToResourceSchema(Schema s) throws IOException {
    return avroSchemaToResourceSchema(s, 
        Sets.<String>newHashSet(), 
        Maps.<String,ResourceSchema>newHashMap());
  }  

  /**
   * Translates a single Avro schema field to a Resource Schema field (for Pig)
   * @param s The avro schema field for which to determine the type
   * @return the corresponding pig resource schema field
   */  
  private static ResourceSchema.ResourceFieldSchema fieldToResourceFieldSchema(Field f, 
      Set<String> namesInStack, Map<String,ResourceSchema> alreadyDefinedSchemas) 
    throws IOException {
    ResourceSchema.ResourceFieldSchema rf = 
        new ResourceSchema.ResourceFieldSchema();
    rf.setName(f.name());
    Schema fieldSchema = f.schema();
    if (fieldSchema.getType() == Type.FIXED) {
      rf.setDescription(fieldSchema.toString());
    } else 
       rf.setDescription(f.doc());
    if (isNullableUnion(fieldSchema))
      fieldSchema = removeNullableUnion(fieldSchema);
    byte pigType = getPigType(fieldSchema);
    rf.setType(pigType);
    switch (pigType) {
      case DataType.BAG:
        {
          ResourceSchema innerResourceSchema = avroSchemaToResourceSchema(
              fieldSchema.getElementType(),namesInStack, alreadyDefinedSchemas);
          ResourceSchema bagSchema = new ResourceSchema();
          ResourceSchema.ResourceFieldSchema[] bagSchemaFields = new ResourceSchema.ResourceFieldSchema[1];
          bagSchemaFields[0] = new ResourceSchema.ResourceFieldSchema();
          bagSchemaFields[0].setType(DataType.TUPLE);
          bagSchemaFields[0].setName(fieldSchema.getElementType().getName());
          bagSchemaFields[0].setSchema(innerResourceSchema);
          bagSchemaFields[0].setDescription(fieldSchema.getDoc());
          bagSchema.setFields(bagSchemaFields);
          rf.setSchema(bagSchema);
        }
        break;
      case DataType.MAP:
        {
          Schema mapAvroSchema = fieldSchema.getValueType();
          ResourceSchema mapSchema = new ResourceSchema();
          ResourceSchema.ResourceFieldSchema[] mapSchemaFields = new ResourceSchema.ResourceFieldSchema[1];
          if (mapAvroSchema.getType() == Type.RECORD) {
             ResourceSchema innerResourceSchema = avroSchemaToResourceSchema(
                fieldSchema.getValueType(), namesInStack, alreadyDefinedSchemas);
             mapSchemaFields[0] = new ResourceSchema.ResourceFieldSchema();
             mapSchemaFields[0].setType(DataType.TUPLE);
             mapSchemaFields[0].setName(fieldSchema.getElementType().getName());
             mapSchemaFields[0].setSchema(innerResourceSchema);
             mapSchemaFields[0].setDescription(fieldSchema.getDoc());
          } else {
            mapSchemaFields[0] = new ResourceSchema.ResourceFieldSchema();
            mapSchemaFields[0].setType(getPigType(mapAvroSchema));
            // mapSchemaFields[0].setName(fieldSchema.getValueType().getName());
          }
          mapSchema.setFields(mapSchemaFields);
          rf.setSchema(mapSchema);
        }
        break;
      case DataType.TUPLE:
        if (alreadyDefinedSchemas.containsKey(fieldSchema.getFullName())) {
          rf.setSchema(alreadyDefinedSchemas.get(fieldSchema.getFullName()));
        } else {
          ResourceSchema innerResourceSchema = avroSchemaToResourceSchema(
              fieldSchema, namesInStack, alreadyDefinedSchemas);
          rf.setSchema(innerResourceSchema);
          alreadyDefinedSchemas.put(fieldSchema.getFullName(), innerResourceSchema);
        }
        break;
    }
    return rf;
  }
  
  /**
   * Translates an Avro schema to a Resource Schema (for Pig). Internal method.
   * @param s The avro schema for which to determine the type
   * @return the corresponding pig schema
   */  
  private static ResourceSchema avroSchemaToResourceSchema(Schema s, 
        Set<String> namesInStack, Map<String,ResourceSchema> alreadyDefinedSchemas)
      throws IOException {

    ResourceSchema.ResourceFieldSchema[] resourceFields = null;
    switch (s.getType()) {
      case RECORD:
        if (namesInStack.contains(s.getFullName()))
          throw new IOException("Pig doesn't support recursive schema definitions; while processing" 
              + s.toString() + " encountered "
              + s.getFullName() + " which was already seen in this stack: "
              + namesInStack.toString() + "\n");
        namesInStack = Sets.newHashSet(namesInStack);
        namesInStack.add(s.getFullName());
        resourceFields = new ResourceSchema.ResourceFieldSchema[s.getFields().size()];
        for (Field f : s.getFields()) {
          resourceFields[f.pos()] = fieldToResourceFieldSchema(f, namesInStack, alreadyDefinedSchemas);
        }
        break;
      default:
          throw new IOException("avroSchemaToResourceSchema only processes records");
    }
    ResourceSchema rs = new ResourceSchema();
    rs.setFields(resourceFields);

    return rs;
  }
    
  public static Schema resourceSchemaToAvroSchema(ResourceSchema rs, 
      String recordName, String recordNameSpace, Map<String,List<Schema>> definedRecordNames) throws IOException {

    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    
    for (ResourceSchema.ResourceFieldSchema rfs : rs.getFields()) {
      Schema fieldSchema = resourceFieldSchemaToAvroSchema(
          rfs.getName(), rfs.getType(),
          rfs.getDescription().equals("autogenerated from Pig Field Schema") ? null : rfs.getDescription(),
              rfs.getSchema(), definedRecordNames);
      fields.add(new Schema.Field(
            rfs.getName(),
            fieldSchema, 
            rfs.getDescription().equals("autogenerated from Pig Field Schema") ? null : rfs.getDescription(),
            null)
      );
    }
 
    Schema newSchema = Schema.createRecord(recordName, null, recordNameSpace, false);
    newSchema.setFields(fields);
    return newSchema;
  }

  private static Schema createNullableUnion(Schema in) {
    return Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), in));
  }

  private static Schema createNullableUnion(Type t) {
    return createNullableUnion(Schema.create(t));
  }

  
  private static Schema resourceFieldSchemaToAvroSchema(String name, byte type, String description,
      ResourceSchema schema, Map<String,List<Schema>> definedRecordNames)  throws IOException {
    switch (type) {
    case DataType.BAG:
      return createNullableUnion(Schema.createArray(resourceSchemaToAvroSchema(
          schema.getFields()[0].getSchema(), name, null, definedRecordNames)));
    case DataType.BIGCHARARRAY:
      return createNullableUnion(Type.STRING);
    case DataType.BOOLEAN:
      return createNullableUnion(Type.BOOLEAN);
    case DataType.BYTEARRAY:
      Schema fixedSchema;
      try {
        fixedSchema = (new Schema.Parser()).parse(description);
      } catch (Exception e) {
        fixedSchema = null;
      }
      if (fixedSchema == null)
          return createNullableUnion(Type.BYTES);
        else
          return createNullableUnion(fixedSchema);
    case DataType.CHARARRAY:
      return createNullableUnion(Type.STRING);
    case DataType.DOUBLE:
      return createNullableUnion(Type.DOUBLE);
    case DataType.FLOAT:
      return createNullableUnion(Type.FLOAT);
    case DataType.INTEGER:
      return createNullableUnion(Type.INT);
    case DataType.LONG:
      return createNullableUnion(Type.LONG);
    case DataType.MAP:
      byte innerType = schema.getFields()[0].getType();
      String desc = schema.getFields()[0].getDescription();
      if (desc.equals("autogenerated from Pig Field Schema"))
        desc = null;
      Schema innerSchema;
      if (DataType.isComplex(innerType)) {        
        innerSchema = createNullableUnion(Schema.createMap(resourceSchemaToAvroSchema(
            schema.getFields()[0].getSchema(), desc, null, definedRecordNames)));
      } else {
        innerSchema = createNullableUnion(Schema.createMap(resourceFieldSchemaToAvroSchema(
            name,innerType, desc, null, definedRecordNames)));
      }
      return innerSchema;      
    case DataType.NULL:
      return createNullableUnion(Type.NULL);
    case DataType.TUPLE:
      Schema returnSchema = createNullableUnion(resourceSchemaToAvroSchema(schema, name, null, definedRecordNames));
      if (definedRecordNames.containsKey(name)) {
        List<Schema> schemaList = definedRecordNames.get(name);
        boolean notfound = true;
        for (Schema cachedSchema : schemaList) {
           if (returnSchema.equals(cachedSchema))
             notfound = false;
             break;
         }
        if (notfound) {
          returnSchema = createNullableUnion(resourceSchemaToAvroSchema(
               schema, name + "_" + new Integer(schemaList.size()).toString(), null, definedRecordNames));
           definedRecordNames.get(name).add(returnSchema);
        }
      } else {
        definedRecordNames.put(name, Lists.newArrayList(returnSchema));
      } 
      return returnSchema;
    case DataType.BYTE:
    case DataType.ERROR:
    case DataType.GENERIC_WRITABLECOMPARABLE:
    case DataType.INTERNALMAP:
    case DataType.UNKNOWN:
    default:
      throw new IOException("Don't know how to encode type " + DataType.findTypeName(type) + " in schema " + schema.toString()+ "\n");        
    }
  }
  
  private static boolean isNullableUnion(Schema s) {
    return (   s.getType() == Type.UNION 
            && s.getTypes().size() == 2
            && (s.getTypes().get(0).getType() == Type.NULL || s.getTypes().get(1).getType() == Type.NULL)
        );
  }
  
  private static Schema removeNullableUnion(Schema s) {
    if (s.getType() == Type.UNION) {
      List<Schema> types = s.getTypes();
      for (Schema t : types)
        if (t.getType() != Type.NULL)
          return t;
    }
    return s;
  }
  
  /**
   * Packs a Pig Tuple into an Avro record
   * @param t the Pig tuple to pack into the avro object
   * @param s The avro schema for which to determine the type
   * @return the avro record corresponding to the input tuple
   */  
  public static GenericData.Record packIntoAvro(Tuple t, Schema s) throws IOException {
    
    try {
      GenericData.Record record = new GenericData.Record(s);
      for (Field f : s.getFields()) {
        Object o = t.get(f.pos());
        Schema innerSchema = f.schema();
        if (isNullableUnion(innerSchema)) {
          if (o == null) {
            record.put(f.pos(), null);
            continue;
          }
          innerSchema = removeNullableUnion(innerSchema);
        }
        switch(innerSchema.getType()) {
        case RECORD:
          record.put(f.pos(), packIntoAvro((Tuple)o, innerSchema)); 
          break;
        case ARRAY:
          record.put(f.pos(), packIntoAvro((DataBag)o, innerSchema));
          break;
        case BYTES:
          record.put(f.pos(), ByteBuffer.wrap(((DataByteArray)o).get()));
          break;
        case FIXED:
          record.put(f.pos(), new GenericData.Fixed(innerSchema, ((DataByteArray)o).get()));
          break;
        default:
          record.put(f.pos(), o);
        }
      }
      return record;
    } catch (Exception e) {
      e.printStackTrace();
      System.err.printf("error processing Tuple %s, Schema %s\n", t.toDelimitedString(","), s.toString());
      throw new IOException(e);
    }      
  }

  /**
   * Packs a Pig DataBag into an Avro array
   * @param db the Pig databad to pack into the avro array
   * @param s The avro schema for which to determine the type
   * @return the avro array corresponding to the input bag
   */  
  public static GenericData.Array<GenericData.Record> packIntoAvro(DataBag db, Schema s) throws IOException {

    try {
      GenericData.Array<GenericData.Record> array 
        = new GenericData.Array<GenericData.Record>(new Long(db.size()).intValue(), s);
      for (Tuple t : db) {
        array.add(packIntoAvro(t, s.getElementType()));
      }
      return array;
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }      
  }

  /**
   * Takes an Avro Schema and a Pig RequiredFieldList and returns a new schema with
   * only the requried fields, or no if the function can't extract only those fields. Useful
   * for push down projections
   * @param rfl the Pig required field list
   * @param oldschema The avro schema for which to determine the type
   * @return the new schema, or null
   */  
  public static Schema newSchemaFromRequiredFieldList(Schema oldSchema, RequiredFieldList rfl) {
    return newSchemaFromRequiredFieldList(oldSchema, rfl.getFields());
  }
  
  public static Schema newSchemaFromRequiredFieldList(Schema oldSchema, List<RequiredField> rfl) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (RequiredField rf : rfl) {
      Schema.Field f = oldSchema.getField(rf.getAlias());
      if (f == null)
        return null;
      try {
        if (getPigType(f.schema()) != rf.getType())
          return null;
      } catch (ExecException e) {
        e.printStackTrace();
        return null;
      }
      if (rf.getSubFields() == null)
        fields.add(new Schema.Field(
            f.name(), f.schema(), f.doc(), f.defaultValue()));
      else {
        Schema innerSchema = newSchemaFromRequiredFieldList(f.schema(), rf.getSubFields());
        if (innerSchema == null)
          return null;
        else
          fields.add(new Schema.Field(
              f.name(), innerSchema, f.doc(), f.defaultValue()));          
      }
    }
    
    Schema newSchema = Schema.createRecord(
        oldSchema.getName(),
        "subset of fields from " + oldSchema.getName() + "; " + oldSchema.getDoc(),
        oldSchema.getNamespace(), 
        false);

    newSchema.setFields(fields);
    return newSchema;
  }
  
}

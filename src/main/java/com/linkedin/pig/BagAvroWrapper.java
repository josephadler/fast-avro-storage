package com.linkedin.pig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class BagAvroWrapper<T> implements DataBag {

  GenericArray<T> theArray;
  
  public BagAvroWrapper(GenericArray<T> a) {
    theArray = a;
  }

  @Override
  public long spill() {
    return 0;
  }

  @Override
  public long getMemorySize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // TODO Auto-generated method stub
        
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public int compareTo(Object o) {
    return GenericData.get().compare(theArray, o, theArray.getSchema());
  }

  @Override
  public long size() {
    return theArray.size();
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public boolean isDistinct() {
    return false;
  }

  @Override
  public Iterator<Tuple> iterator() {
      return Iterators.transform(theArray.iterator(),           
          new Function<T, Tuple> () {
            @Override
            public Tuple apply(T arg0) {
              if (arg0 instanceof IndexedRecord)
                return new TupleAvroWrapper<IndexedRecord>((IndexedRecord) arg0);
              else
                return TupleFactory.getInstance().newTuple(arg0);
            }});
  }

  @SuppressWarnings("unchecked")
  @Override
  public void add(Tuple t) {
    theArray.add((T) t);
  }

  @Override
  public void addAll(DataBag b) {
    for (Tuple t : b)
      add(t);
  }

  @Override
  public void clear() {
    theArray.clear();
  }

  @Override
  public void markStale(boolean stale) {
    // TODO Auto-generated method stub

  }

}

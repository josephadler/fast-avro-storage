/**
 * 
 */
package com.linkedin.pig;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.avro.util.Utf8;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.collect.Sets;
import com.google.common.base.Function;

/**
 * @author jadler
 * 
 * Wrapper for map objects, so we can translate UTF8 objects to Strings if we encounter them
 *
 */
public class MapAvroWrapper implements Map<CharSequence, Object> {

  Map<CharSequence, Object> innerMap;

  public MapAvroWrapper(Map<CharSequence, Object> m) {
    innerMap = m;
  }
  
  @Override
  public int size() {
    return innerMap.size();
  }

  @Override
  public boolean isEmpty() {
    return innerMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return innerMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return innerMap.containsValue(value);
  }

  @Override
  public Object get(Object key) {
    Object v = innerMap.get(key);
    if (v instanceof Utf8)
      return v.toString();
    else
      return v;
  }

  @Override
  public Object put(CharSequence key, Object value) {
    return innerMap.put(key, value);
  }

  @Override
  public Object remove(Object key) {
    return innerMap.remove(key);
  }

  @Override
  public void putAll(Map<? extends CharSequence, ? extends Object> m) {
    innerMap.putAll(m);
  }

  @Override
  public void clear() {
    innerMap.clear();
  }

  @Override
  public Set<CharSequence> keySet() {
    return innerMap.keySet();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Collection<Object> values() {
    return Collections2.transform(innerMap.values(), new Function() {
      @Override
      public Object apply(Object v) {
        if (v instanceof Utf8)
          return v.toString();
        else
          return v;
      }});    
  }

  @Override
  public Set<java.util.Map.Entry<CharSequence, Object>> entrySet() {

    Set<java.util.Map.Entry<CharSequence, Object>> theSet = Sets.newHashSetWithExpectedSize(innerMap.size());
    for (java.util.Map.Entry<CharSequence, Object> e : innerMap.entrySet()) {
      CharSequence k = e.getKey();
      Object v = e.getValue();
      if (k instanceof Utf8)
        k = k.toString();
      if (v instanceof Utf8)
        v = v.toString();
      theSet.add(new AbstractMap.SimpleEntry<CharSequence, Object>(k,v));
    }        
    return theSet;    
    
  }

}

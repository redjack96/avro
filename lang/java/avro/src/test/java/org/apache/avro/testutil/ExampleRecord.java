/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.testutil;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Implemento una semplice classe serializzabile come RECORD per Avro
 */
public class ExampleRecord implements IndexedRecord, FakeRecord {
  int first;
  String second;

  public ExampleRecord(){
    this.first = 1;
    this.second = "2";
  }

  public ExampleRecord(int first, String second) {
    this.first = first;
    this.second = second;
  }

  public String getFieldName(int i){
    return FakeRecord.getFieldNameStatic(i, this);
  }

  public static String getFieldNameStatic(int i){
    return FakeRecord.getFieldNameStatic(i, new ExampleRecord(1, "2"));
  }


  /**
   * @return Lo schema avro relativo all'intera istanza.
   */
  @Override
  public Schema getSchema() {
    return getSchemaStatic();
  }

  /**
   * Questo metodo aggiorna il valore del campo in posizione i
   * @param i la posizione del campo
   * @param v il valore da assegnare al campo
   */
  @Override
  public void put(int i, Object v) {
    try {
      Field field = this.getClass().getDeclaredFields()[i];
      field.set(this, v);
    }catch(Exception e){
      e.printStackTrace();
    }
  }

  /**
   * Questo metodo restituisce il valore del campo in posizione i
   * @param i la posizione del campo
   * @return il valore del campo.
   */
  @Override
  public Object get(int i) {
    try {
      Field field = this.getClass().getDeclaredFields()[i];
      return field.get(this);
    } catch(Exception e){
      return null;
    }
  }

  /**
   * Metodo equals, per poter fare il confronto tra due oggetti TrialRecord
   * all'interno del metodo assertEqual().
   * @param o un altro TrialRecord.
   * @return true se i contenuti dei due TrialRecord sono uguali.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExampleRecord that = (ExampleRecord) o;
    return first == that.first && second.equals(that.second);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  public static Map<String, Schema> getMap(){
    Map<String, Schema> a = new HashMap<>();
    a.put(ExampleRecord.class.getName(), getSchemaStatic());
    return a;
  }

  public static Schema getSchemaStatic(){
    List<Schema.Field> schemaFields = new ArrayList<>();
    schemaFields.add(new Schema.Field("first", Schema.create(Schema.Type.INT)));
    schemaFields.add(new Schema.Field("second", Schema.create(Schema.Type.STRING)));
    return Schema.createRecord("TrialRecord", "", "", false, schemaFields);
  }
}

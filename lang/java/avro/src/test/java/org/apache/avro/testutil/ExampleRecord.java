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

import java.util.*;

/**
 * Implemento una semplice classe serializzabile come RECORD per Avro
 */
public class ExampleRecord implements IndexedRecord {
  int first;
  String second;

  public ExampleRecord(int first, String second) {
    this.first = first;
    this.second = second;
  }

  /**
   * @return Lo schema avro relativo all'intera istanza.
   */
  @Override
  public Schema getSchema() {
    return getSchemaStatic();
  }

  @Override
  public void put(int i, Object v) {
    this.first = i;
    this.second = v.toString();
  }

  @Override
  public Object get(int i) {
    return this.second;
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

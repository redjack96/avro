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
package org.apache.avro.reflect;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.testutil.ExampleRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Type;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Test parametrico del metodo GenericData::deepCopy(). Il test riceve due parametri,
 * uno Schema non nullo e un oggetto Java. Se lo schema e l'oggetto sono compatibili, il
 * metodo restituisce una COPIA PROFONDA dell'oggetto dato in input.
 * Questo test verifica che l'oggetto copiato sia identico a quello iniziale.
 * <p>
 * N.B.: Per far funzionare la build, senza tener conto dello stile delle indentazioni
 * di Avro, ho disabilitato i plugin spotless e check-style
 * <p>
 * Per eseguire solo questo test:
 * mvn test -Dtest=TestCreateSchema -pl lang/java/avro
 */
@RunWith(Parameterized.class)
public class TestCreateSchema {
  private static ReflectData reflectData;

  private final Type type;
  private final Map<String, Schema> mapStringSchema;
  private final Object expectedSchemaOrThrow;

  public TestCreateSchema(Class<?> type, Map<String, Schema> mapStringSchema, Object expectedSchemaOrThrow) {
    this.type = type;
    this.mapStringSchema = mapStringSchema;
    this.expectedSchemaOrThrow = expectedSchemaOrThrow;
  }

  @BeforeClass
  public static void beforeClass() {
    reflectData = ReflectData.get();
  }

  @AfterClass
  public static void afterClass() {
    reflectData = null;
  }

  @Parameterized.Parameters(name = "test create-schema n° {index}")
  public static Collection<Object[]> data() {
    return configure();
  }

  private static Collection<Object[]> configure() {
    return Arrays.asList(new Object[][]{
      // tipo compatibile con schema, map nullo
      {Integer.class, null, Schema.create(Schema.Type.INT)},
      // tipo compatibile con schema, map vuoto
      {Integer.class, new HashMap<>(), Schema.create(Schema.Type.INT)},
      // tipo compatibile con schema, map non vuoto
      {String.class, initializeMap(Schema.Type.STRING), Schema.create(Schema.Type.STRING)},
      // tipo compatibile con schema, map con schema nullo.
      {String.class, initializeNoSchemaMap(Schema.Type.STRING), Schema.create(Schema.Type.STRING)},
      // tipo NON compatibile con schema, map nullo
      {ArrayList.class, null, new AvroRuntimeException("errore arrayList")},
      // tipo NON compatibile con schema, map vuoto
      {ArrayList.class, new HashMap<>(), new AvroRuntimeException("errore arrayList map vuoto")},
      // tipo NON compatibile con schema, map pieno
      {LinkedList.class, initializeMap(Schema.Type.NULL), new AvroRuntimeException("errore arrayList map pieno")},
      {String.class, initializeMap(Schema.Type.INT), Schema.create(Schema.Type.STRING)},
      // tipo NON compatibile con schema, map pieno ma con schema nullo
      {LinkedList.class, initializeNoSchemaMap(Schema.Type.NULL), new AvroRuntimeException("errore arrayList map pieno senza schema")},
      {String.class, initializeNoSchemaMap(Schema.Type.INT), Schema.create(Schema.Type.STRING)},
      // record
      {ExampleRecord.class, ExampleRecord.getMap(), ExampleRecord.getSchemaStatic()}
    });
  }

  /**
   * Il test verifica che il metodo createSchema() restituisca lo schema atteso dato
   * il set corrente dei parametri di input.
   */
  @Test
  public void createSchema() {
    if (this.expectedSchemaOrThrow instanceof Schema) {
      assertEquals(this.expectedSchemaOrThrow, reflectData.createSchema(this.type, this.mapStringSchema));
    } else {
      assertThrows(AvroRuntimeException.class, () -> reflectData.createSchema(this.type, this.mapStringSchema));
    }
  }

  private static Map<String, Schema> initializeMap(Schema.Type type) {
    Map<String, Schema> map = new HashMap<>();
    map.put("Schema-" + type.getName(), Schema.create(type));
    return map;
  }

  private static Map<String, Schema> initializeNoSchemaMap(Schema.Type type) {
    Map<String, Schema> map = new HashMap<>();
    map.put("Schema-" + type.getName(), null);
    return map;
  }
}

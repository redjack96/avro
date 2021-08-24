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

import org.apache.avro.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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

  private Class<?> type;
  private Map<String, Schema> mapStringSchema;
  private Schema expectedSchema;

  public TestCreateSchema(Class<?> type, Map<String, Schema> mapStringSchema, Schema expectedSchema) {
    this.type = type;
    this.mapStringSchema = mapStringSchema;
    this.expectedSchema = expectedSchema;
  }

  @BeforeClass
  public static void beforeClass() {
    reflectData = ReflectData.get();
  }

  @AfterClass
  public static void afterClass() {
    reflectData = null;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return configure();
  }

  private static Collection<Object[]> configure() {
    return Arrays.asList(new Object[][]{
      {Integer.class, null, Schema.create(Schema.Type.INT)},
      // {null, initializeMap(Schema.Type.INT), Schema.create(Schema.Type.INT)},
      {Void.class, new HashMap<>(), Schema.create(Schema.Type.NULL)},
      {String.class, initializeMap(Schema.Type.STRING), Schema.create(Schema.Type.STRING)},

    });
  }

  @Test
  public void createSchema() {
    assertEquals(this.expectedSchema, reflectData.createSchema(this.type, this.mapStringSchema));
  }

  private static Map<String, Schema> initializeMap(Schema.Type type) {
    Map<String, Schema> map = new HashMap<>();
    map.put("Schema-" + type.getName(), Schema.create(type));
    return map;
  }
}

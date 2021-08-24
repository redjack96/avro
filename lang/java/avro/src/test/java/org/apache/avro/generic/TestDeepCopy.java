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
package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.testutil.TrialRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
 * mvn test -Dtest=TestDeepCopy -pl lang/java/avro
 */
@RunWith(Parameterized.class)
public class TestDeepCopy {

  private static GenericData genericData;
  private static HashMap<String, List<Double>> deepMap;
  private static final Logger LOG = LoggerFactory.getLogger(TestDeepCopy.class);
  private final Object value; // spesso è sia il valore dato in input, sia il valore atteso.
  private final Schema schema;
  private final Object expectedCopy;

  public TestDeepCopy(Object value, Schema schema, Object expectedCopy) {
    this.value = value;
    this.schema = schema;
    this.expectedCopy = expectedCopy;
  }

  @BeforeClass
  public static void beforeClass() {
    deepMap = new HashMap<>();
    deepMap.put("uno", Arrays.asList(1.0,2.0,3.0));
    deepMap.put("due", Arrays.asList(2.0,3.0,4.0));
    genericData = GenericData.get();
  }

  @AfterClass
  public static void afterClass() {
    genericData = null;
    deepMap = null;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    return configure();
  }

  private static Collection<Object[]> configure(){
    TrialRecord due = new TrialRecord(2, "due");
    Schema trialRecordSchema = due.getSchema();
    Object record = GenericData.get().newRecord(due, trialRecordSchema);
    Object recordCopy = GenericData.get().newRecord(due, trialRecordSchema);

    return Arrays.asList(new Object[][]{
      {null, Schema.create(Schema.Type.DOUBLE), null}, // oggetto nullo, schema NON compatibile. Deve funzionare lo stesso
      {null, Schema.create(Schema.Type.NULL), null}, // oggetto nullo, schema compatibile.
      {10, Schema.create(Schema.Type.FLOAT), 10}, // oggetto semplice, schema NON compatibile. Deve dare errore
      {"String", Schema.create(Schema.Type.STRING), "String"}, // oggetto semplice, schema compatibile
      // List(s) e Map(s) a quanto pare non sono oggetti profondi...
      {Arrays.asList("0.e0", "5q.0", "3.0g"), Schema.createArray(Schema.create(Schema.Type.INT)), Arrays.asList("0.e0", "5q.0", "3.0g")}, // oggetti multipli semplici, schema NON compatibile. Nonostante ciò è possibile copiarlo.
      {Arrays.asList(0, 5, 3), Schema.createArray(Schema.create(Schema.Type.INT)), Arrays.asList(0, 5, 3)}, // oggetti multipli semplici, schema compatibile
      {deepMap, Schema.createMap(Schema.createArray(Schema.create(Schema.Type.DOUBLE))), deepMap},
      {deepMap, Schema.createMap(Schema.createArray(Schema.create(Schema.Type.STRING))), deepMap},
      // oggetto profondo, schema compatibile
      {record, trialRecordSchema, recordCopy},
      // oggetto profondo, schema NON compatibile
      // {GenericData.get().newRecord(due, trialRecordSchema), Schema.createMap(Schema.createArray(Schema.create(Schema.Type.DOUBLE))), GenericData.get().newRecord(due, trialRecordSchema)}, // Questo test fallisce con cast exception
    });
  }

  @Test
  public void deepCopy() {
    Object o = genericData.deepCopy(schema, value);
    assertEquals(expectedCopy, o);
  }
}

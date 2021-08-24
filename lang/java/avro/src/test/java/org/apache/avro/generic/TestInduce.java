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

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.testutil.TrialRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Test parametrico del metodo GenericData::induce(). Il test riceve un solo parametro,
 * un oggetto Java. Da esso produce uno Schema compatibile.
 * Questo test verifica che il metodo produca lo Schema corretto in base al dato in input.
 * <p>
 * N.B.: Per far funzionare la build, senza tener conto dello stile delle indentazioni
 * di Avro, ho disabilitato i plugin spotless e check-style
 * <p>
 * Per eseguire solo questo test:
 * mvn test -Dtest=TestInduce -pl lang/java/avro
 */
@RunWith(Parameterized.class)
public class TestInduce {
  private static GenericData genericData;
  private final Object datum;
  private final Schema expectedSchema;

  public TestInduce(Object datum, Schema expectedSchema) {
    this.datum = datum;
    this.expectedSchema = expectedSchema;
  }

  @BeforeClass
  public static void beforeClass() {
    genericData = GenericData.get();
  }

  @AfterClass
  public static void afterClass() {
    genericData = null;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    return configure();
  }

  private static Collection<Object[]> configure(){
    Map<String, Integer> trialMap = new HashMap<>();
    trialMap.put("uno", 1);
    trialMap.put("due", 2);
    TrialRecord trialRecord = new TrialRecord(1, "uno");

    return Arrays.asList(new Object[][]{
      // null produce uno schema nullo
      {null, Schema.create(Schema.Type.NULL)},
      // oggetti che producono schemi con dati singoli
      {5, Schema.create(Schema.Type.INT)},
      {"5", Schema.create(Schema.Type.STRING)},
      // {b, Schema.create(Schema.Type.BYTES)},
      // oggetti che producono schemi con dati aggregati
      {Arrays.asList(4.6f, 6.5f, 3.2f, 0.0f), Schema.createArray(Schema.create(Schema.Type.FLOAT))}, // le liste devono contenere oggetti dello stesso tipo
      {trialMap, Schema.createMap(Schema.create(Schema.Type.INT))},
      {trialRecord, trialRecord.getSchema()},
      // oggetti non compatibili (es. gli array Java)
      {new int[]{2, 3}, null},
      {new BigDecimal("2.4"), null},
    });
  }

  @Test
  public void induce() {
    if (expectedSchema != null) {
      assertEquals("I due schemi non coincidono.", expectedSchema, genericData.induce(datum));
    } else {
      assertThrows(AvroTypeException.class, () -> genericData.induce(datum));
    }
  }
}

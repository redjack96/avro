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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Test parametrico del metodo GenericData::validate(). Il test riceve due parametri,
 * uno Schema non nullo e un oggetto Java. Se lo schema e l'oggetto sono compatibili, il
 * metodo restituisce true. Questo test verifica che il metodo abbia l'esito corretto
 * in base alla combinazione tra Schema e dato.
 *
 * N.B.: Per far funzionare la build, senza tener conto dello stile delle indentazioni
 * di Avro, ho disabilitato i plugin spotless e check-style
 *
 * Per eseguire solo questo test:
 *   mvn test -Dtest=TestValidate -pl lang/java/avro
 */
@RunWith(Parameterized.class)
public class TestValidate {

  private static GenericData genericData;
  // Di seguito i parametri del test
  private final Schema schema;
  private final Object datum;
  private final boolean expectedValidation;

  public TestValidate(Schema schema, Object datum, boolean expectedValidation) {
    this.schema = schema;
    this.datum = datum;
    this.expectedValidation = expectedValidation;
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
    return Arrays.asList(new Object[][]{
      {Schema.create(Schema.Type.STRING), "Stringa", true},
      {Schema.create(Schema.Type.FLOAT), 4, false},
      {Schema.create(Schema.Type.DOUBLE), null, false},
      {Schema.createArray(Schema.create(Schema.Type.INT)), Arrays.asList(0, 5, 3), true},
      {Schema.createUnion(Schema.create(Schema.Type.BOOLEAN)), "ciao", false},
      {Schema.createMap(Schema.create(Schema.Type.NULL)), Arrays.asList(null, null, null), false},
      {Schema.create(Schema.Type.NULL), null, true},
      {Schema.create(Schema.Type.NULL), new ArrayList<Void>(), false},
    });
  }

  /**
   * Il test verifica il funzionamento del metodo validate()
   * su una singola combinazione di schema e dato.
   */
  @Test
  public void validate() {
    assertEquals(String.format("Il dato %s doveva essere %s per lo schema.", this.datum, this.expectedValidation ? "valido " : "non valido "),
      this.expectedValidation, genericData.validate(this.schema, this.datum));
  }

}

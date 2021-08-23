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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

/**
 * N.B.: Per far funzionare la build, senza tener conto dello stile delle indentazioni
 * di Avro, ho disabilitato il plugin spotless e check-style
 */
@RunWith(Parameterized.class)
public class TestValidate {

  // TODO: decidere se deve essere static o meno.
  private final GenericData genericData;
  private final Schema schema;
  private final Object datum;
  private final boolean expectedValidation;

  public TestValidate(GenericData genericData, Schema schema, Object datum, boolean expectedValidation) {
    this.genericData = genericData;
    this.schema = schema;
    this.datum = datum;
    this.expectedValidation = expectedValidation;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    GenericData genData = new GenericData();
    return Arrays.asList(new Object[][]{
      {genData, Schema.createArray(Schema.create(Schema.Type.INT)), Arrays.asList(0, 5, 3), true},
      {genData, Schema.create(Schema.Type.STRING), "Stringa", true},
    });
  }

  /**
   * mvn test -Dtest=TestGenericData.validate -pl lang/java/avro
   */
  @Test
  public void validate() {
    assertEquals(String.format("Il GenericData doveva essere %s", this.expectedValidation ? "valido." : "non valido."),
      genericData.validate(this.schema, this.datum), this.expectedValidation);
    System.out.println("Ho validato lo schema");
  }

}

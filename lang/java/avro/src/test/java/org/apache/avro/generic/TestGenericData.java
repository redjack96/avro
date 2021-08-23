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
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Per eseguire i test, usa i comandi seguenti perché molte classi hanno bisogno
 * di file generati durante la compilazione. mvn -Dtest=TestGenericData test
 */
public class TestGenericData {

  private static GenericData genericData;

  @BeforeClass
  public static void setUpAll() {
    genericData = new GenericData();
  }

  /**
   * mvn test -Dtest=TestGenericData.validate -pl lang/java/avro
   */
  @Test
  public void validate() {
    Schema s = Schema.create(Schema.Type.STRING); // Array non è valido
    assertFalse(genericData.validate(s, new Integer[] { 0, 5, 3 }));
    assertTrue(genericData.validate(s, "new Integer[] { 0, 5, 3 }"));
    System.out.println("Ho validato lo schema");
  }

  /**
   * Anche questo sembra buono, ma cosa fa???
   */
  @Test
  public void induce() {
  }

  // Sembra buono
  @Test
  public void resolveUnion() {
  }

  // Decente
  @Test
  public void deepCopy() {
  }

  @Test
  public void newArray() {
  }

}

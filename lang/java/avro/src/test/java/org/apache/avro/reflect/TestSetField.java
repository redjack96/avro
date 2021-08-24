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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestSetField {

  private static ReflectData reflectData;

  private final Object record;
  private final String fieldName;
  private final int fieldIndex;
  private final Object fieldValue;

  public TestSetField(Object record, String fieldName, int fieldIndex, Object fieldValue) {
    this.record = record;
    this.fieldName = fieldName;
    this.fieldIndex = fieldIndex;
    this.fieldValue = fieldValue;
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
  public static Collection<Object[]> getParameters() {
    return Arrays.asList(new Object[][]{

    });
  }

  @Test
  public void setField() {

  }
}

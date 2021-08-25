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

import java.util.Random;

/**
 * Una semplice classe che contiene dati, ma che non è un IndexedRecord.
 * Usata in TestSetField.
 */
public class NotARecord implements FakeRecord{
  private String name;
  private float weight;
  private int quantity;
  private boolean available;

  private static final Random r = new Random();
  public NotARecord() {
    name = "abc";
    weight = r.nextFloat();
    quantity = r.nextInt();
    available = r.nextBoolean();
  }

  public String getFieldName(int i){
    return FakeRecord.getFieldNameStatic(i, this);
  }

  public static String getFieldNameStatic(int i){
    return FakeRecord.getFieldNameStatic(i, new NotARecord());
  }
}

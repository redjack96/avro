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

import org.apache.avro.testutil.ExampleRecord;
import org.apache.avro.testutil.NotARecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

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
 * mvn test -Dtest=TestSetField -pl lang/java/avro
 */
@RunWith(Parameterized.class)
public class TestSetField {

  private static ReflectData reflectData;

  private final Object record;
  private final String fieldName;
  private final int fieldIndex;
  private final Object fieldValue;
  private final Class<?> error;

  public TestSetField(Object record, String fieldName, int fieldIndex, Object fieldValue, Class<?> error) {
    this.record = record;
    this.fieldName = fieldName;
    this.fieldIndex = fieldIndex;
    this.fieldValue = fieldValue;
    this.error = error;
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
    NotARecord notARecord = new NotARecord();
    int sizeNotARecord = notARecord.fieldNumber();
    ExampleRecord exampleRecord = new ExampleRecord();
    int sizeExampleRecord = exampleRecord.fieldNumber();
    return Arrays.asList(new Object[][]{
      // 0 Tutto non valido
      {null, null, -1, null, Exception.class},
      // 1 record non valido, il resto valido per la istanza
      {notARecord, notARecord.getFieldName(0), 0, "newName", Exception.class},
      // 2 record valido, ma campo inesistente
      {new ExampleRecord(), "iDoNotExist", 0, "don't care", null},
      // 3 record valido, ma con campo vuoto
      {new ExampleRecord(), "", 0, 10, null},
      // 4 record non valido, campo non esistente ultima posizione
      {new NotARecord(), "iDoNotExist", sizeNotARecord - 1, null, Exception.class},
      // 5 record non valido, campo esistente nell' ultima posizione
      {new NotARecord(), NotARecord.getFieldNameStatic(sizeNotARecord - 1), sizeNotARecord - 1, true, Exception.class},
      // 6 tutto valido. Non mi aspetto errori
      {new ExampleRecord(), ExampleRecord.getFieldNameStatic(0), 0, 777, null},
      // 7 record valido, posizione oltre la massima, il resto è don't care.
      {new ExampleRecord(), "iDoNotExist", sizeExampleRecord, "don't care", Exception.class},
      // 8 tutto valido, field nell'ultima posizione
      {new ExampleRecord(), ExampleRecord.getFieldNameStatic(sizeExampleRecord - 1), sizeExampleRecord - 1, "Nuovo Valore", null},
      // 9 tutto valido, ma il valore è null
      {new ExampleRecord(), ExampleRecord.getFieldNameStatic(sizeExampleRecord - 1), sizeExampleRecord - 1, null, null},
      // 10 position -1, il resto è esistente e compatibile.
      {new ExampleRecord(), ExampleRecord.getFieldNameStatic(0), - 1, 7, Exception.class},


    });
  }

  /**
   * Il test esegue setField con il set di parametri correnti.
   * Se l'ultimo parametro è un' eccezione, si verifica che sia stata lanciata
   * altrimenti si controlla che il campo del record sia stato effettivamente impostato.
   */
  @Test
  public void setField() {
    try {
      // eseguo set field
      Object initialValue = getInitialValue();
      reflectData.setField(record, fieldName, fieldIndex, fieldValue);
      // checkFieldValue(initialValue); // gestisce lui le sue eccezioni
    } catch (Exception e) { // Un' eccezione causata da reflectData.setField
      // verifichiamo che l'eccezione lanciata sia uguale a quella attesa.
      assertSame("E' avvenuta un' eccezione non attesa: " + e.getClass().getSimpleName(), error, Exception.class);
    }


  }

  private Object getInitialValue() {
    try {
      Field field = record.getClass().getDeclaredFields()[fieldIndex];
      if (field == null) return null;
      field.setAccessible(true); // aggiro la protezione private
      return field.get(record);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Verifica se il campo del record viene aggiornato correttamente.
   * Se avviene un errore
   *
   * @param initialValue
   */
  private void checkFieldValue(Object initialValue) {
    // Se ha successo restituisce true, altrimenti un' eccezione.
    if (record == null) return;
    try {
      Field field = record.getClass().getDeclaredFields()[fieldIndex];
      if (field == null) return;
      field.setAccessible(true); // aggiro la protezione private
      Object o = field.get(record);
      // solo se il valore del campo è nullo oppure compatibile col valore da assegnare e non ci aspettiamo errori,
      // controllo che il valore sia stato aggiornato.
      if ((o == null || o.getClass().isInstance(fieldValue)) && error == null) {
        assertEquals("il campo " + fieldName + " non è stato impostato a " + fieldValue, fieldValue, o);
      } else {
        // e che il valore non sia cambiato
        assertEquals("Attenzione, il valore è stato cambiato con un tipo incompatibile!!", initialValue, o);
      }
    } catch (Exception e) {
      assertEquals(String.format("Errore %s - campo scelto = %d/%d. Stack Trace:\n %s", e.getClass().getSimpleName(), fieldIndex + 1, record.getClass().getDeclaredFields().length, Arrays.toString(e.getStackTrace())),
        error, Exception.class);
    }
  }
}

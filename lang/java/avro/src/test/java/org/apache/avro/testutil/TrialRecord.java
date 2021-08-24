package org.apache.avro.testutil;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Implemento una semplice classe serializzabile per Avro
 */
public class TrialRecord implements IndexedRecord {
  int first;
  String second;

  public TrialRecord(int first, String second) {
    this.first = first;
    this.second = second;
  }

  /**
   * @return Lo schema avro relativo all'intera istanza.
   */
  @Override
  public Schema getSchema() {
    List<Schema.Field> schemaFields = new ArrayList<>();
    schemaFields.add(new Schema.Field("first", Schema.create(Schema.Type.INT)));
    schemaFields.add(new Schema.Field("second", Schema.create(Schema.Type.STRING)));
    return Schema.createRecord("TrialRecord", "", "", false, schemaFields);
  }

  @Override
  public void put(int i, Object v) {
    this.first = i;
    this.second = v.toString();
  }

  @Override
  public Object get(int i) {
    return this.second;
  }

  /**
   * Metodo equals, per poter fare il confronto tra due oggetti TrialRecord
   * all'interno del metodo assertEqual().
   * @param o un altro TrialRecord.
   * @return true se i contenuti dei due TrialRecord sono uguali.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TrialRecord that = (TrialRecord) o;
    return first == that.first && second.equals(that.second);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }
}

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableDefinitionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.*;
import org.junit.Test;

import java.sql.JDBCType;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KineticaDatabaseDialectTest extends BaseDialectTest<KineticaDatabaseDialect> {
  @Override
  protected KineticaDatabaseDialect createDialect() {
    return new KineticaDatabaseDialect(sourceConfigWithUrl("jdbc:kinetica://"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Schema.Type.INT8, "TINYINT");
    assertPrimitiveMapping(Schema.Type.INT16, "SMALLINT");
    assertPrimitiveMapping(Schema.Type.INT32, "INTEGER");
    assertPrimitiveMapping(Schema.Type.INT64, "LONG");
    assertPrimitiveMapping(Schema.Type.FLOAT32, "REAL");
    assertPrimitiveMapping(Schema.Type.FLOAT64, "DOUBLE");
    assertPrimitiveMapping(Schema.Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Schema.Type.BYTES, "BYTES");
    assertPrimitiveMapping(Schema.Type.STRING, "VARCHAR");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("TINYINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("LONG", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BYTES", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIME");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE \"myTable\" (\n" + "\"c1\" INTEGER NOT NULL,\n" + "\"c2\" LONG NOT NULL,\n" +
            "\"c3\" VARCHAR NOT NULL,\n" + "\"c4\" VARCHAR NULL,\n" +
            "\"c5\" DATE DEFAULT '2001-03-15',\n" + "\"c6\" TIME DEFAULT '00:00:00.000',\n" +
            "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" + "\"c8\" DECIMAL NULL,\n" +
            "\"c9\" BOOLEAN DEFAULT TRUE,\n" +
            "PRIMARY KEY(\"c1\"))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("id1").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("id2").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("columnA").type("varchar", JDBCType.VARCHAR, String.class);
    builder.withColumn("columnB").type("varchar", JDBCType.VARCHAR, String.class);
    builder.withColumn("columnC").type("varchar", JDBCType.VARCHAR, String.class);
    builder.withColumn("columnD").type("varchar", JDBCType.VARCHAR, String.class);
    TableDefinition tableDefn = builder.build();
    assertEquals(
        "INSERT INTO \"myTable\" /* KI_HINT_UPDATE_ON_EXISTING_PK */  (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
            "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?)",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD, tableDefn)
    );

    String expected = "insert into \"myTable\" /* KI_HINT_UPDATE_ON_EXISTING_PK */ " +
        "(\"id1\",\"id2\",\"columnA\",\"columnB\",\"columnC\",\"columnD\")" +
        " values(?,?,?,?,?,?)";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);

  }


  @Test
  public void upsert() {
    TableId actor = tableId("actor");
    String expected = "insert into \"actor\" /* KI_HINT_UPDATE_ON_EXISTING_PK */ " +
        "(\"actor_id\",\"first_name\",\"last_name\",\"score\") " +
        "values(?,?,?,?)";
    String sql = dialect.buildUpsertQueryStatement(actor, columns(actor, "actor_id"),
        columns(actor, "first_name", "last_name",
            "score"));
    assertEquals(expected, sql);

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    expected = "insert into actor /* KI_HINT_UPDATE_ON_EXISTING_PK */ " +
        "(actor_id,first_name,last_name,score) " +
        "values(?,?,?,?)";
    sql = dialect.buildUpsertQueryStatement(actor, columns(actor, "actor_id"),
        columns(actor, "first_name", "last_name",
            "score"));
    assertEquals(expected, sql);
  }

  @Test
  public void insert() {
    TableId customers = tableId("customers");
    String expected = "INSERT INTO \"customers\"(\"age\",\"firstName\",\"lastName\") VALUES(?,?,?)";
    String sql = dialect.buildInsertStatement(customers, columns(customers),
        columns(customers, "age", "firstName", "lastName"));
    assertEquals(expected, sql);
  }

  @Test
  public void update() {
    TableId customers = tableId("customers");
    String expected =
        "UPDATE \"customers\" SET \"age\" = ?, \"firstName\" = ?, \"lastName\" = ? WHERE " + "\"id\" = ?";
    String sql = dialect.buildUpdateStatement(customers, columns(customers, "id"),
        columns(customers, "age", "firstName", "lastName"));
    assertEquals(expected, sql);
  }

}

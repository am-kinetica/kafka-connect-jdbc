/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class KineticaDatabaseDialect extends GenericDatabaseDialect {
  private static final Logger log = LoggerFactory.getLogger(KineticaDatabaseDialect.class);

  /**
   * The provider for {@link KineticaDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(KineticaDatabaseDialect.class.getSimpleName(), "kinetica");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new KineticaDatabaseDialect(config);
    }
  }

  static final String JSON_TYPE_NAME = "json";
  static final String JSONB_TYPE_NAME = "jsonb";
  static final String UUID_TYPE_NAME = "uuid";

  /**
   * Define the Kinetica datatypes that require casting upon insert/update statements.
   */
  private static final Set<String> CAST_TYPES = Collections.unmodifiableSet(
          Utils.mkSet(
                  UUID_TYPE_NAME
          )
  );

  private static final String KINETICA_UPSERT_HINT = " /* KI_HINT_UPDATE_ON_EXISTING_PK */ ";

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public KineticaDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    super.initializePreparedStatement(stmt);

    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
          return "DATE";
        case org.apache.kafka.connect.data.Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INTEGER";
      case INT64:
        return "LONG";
      case FLOAT32:
        return "REAL";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "VARCHAR";
      case BYTES:
        return "BYTES";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildInsertStatement(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns,
          TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(this.columnValueVariables(definition))
            .of(keyColumns, nonKeyColumns);
    builder.append(")");
    return builder.toString();
  }

  @Override
  public String buildUpdateStatement(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns,
          TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("UPDATE ");
    builder.append(table);
    builder.append(" SET ");
    builder.appendList()
            .delimitedBy(", ")
            .transformedBy(this.columnNamesWithValueVariables(definition))
            .of(nonKeyColumns);
    if (!keyColumns.isEmpty()) {
      builder.append(" WHERE ");
      builder.appendList()
              .delimitedBy(" AND ")
              .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
              .of(keyColumns);
    }
    return builder.toString();
  }

  @Override
  public String buildUpsertQueryStatement(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns,
          TableDefinition definition
  ) {

    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(KINETICA_UPSERT_HINT);
    builder.append(" (");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(this.columnValueVariables(definition))
            .of(keyColumns, nonKeyColumns);
    builder.append(")");

    return builder.toString();
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {

    ExpressionBuilder builder = expressionBuilder();
    builder.append("insert into ");
    builder.append(table);
    builder.append(KINETICA_UPSERT_HINT);
    builder.append("(");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(ExpressionBuilder.columnNames())
        .of(keyColumns, nonKeyColumns);
    builder.append(") values(");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(")");
    return builder.toString();
  }

  @Override
  protected void formatColumnValue(
          ExpressionBuilder builder,
          String schemaName,
          Map<String, String> schemaParameters,
          Schema.Type type,
          Object value
  ) {
    if (schemaName == null && Schema.Type.BOOLEAN.equals(type)) {
      builder.append((Boolean) value ? "TRUE" : "FALSE");
    } else {
      super.formatColumnValue(builder, schemaName, schemaParameters, type, value);
    }
  }

  /**
   * Return the transform that produces an assignment expression each with the name of one of the
   * columns and the prepared statement variable. Kinetica may require the variable to have a
   * type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the assignment expression for use within a prepared
   *         statement; never null
   */
  protected Transform<ColumnId> columnNamesWithValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.appendColumnName(columnId.name());
      builder.append(" = ?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the transform that produces a prepared statement variable for each of the columns.
   * Kinetica may require the variable to have a type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the variable expression for each column; never null
   */
  protected ExpressionBuilder.Transform<ColumnId> columnValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.append("?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the typecast expression that can be used as a suffix for a value variable of the
   * given column in the defined table.
   *
   * <p>This method returns a blank string except for those column types that require casting
   * when set with literal values. For example, a column of type {@code uuid} must be cast when
   * being bound with a {@code varchar} literal, since a UUID value cannot be bound directly.
   *
   * @param tableDefn the table definition; may be null if unknown
   * @param columnId  the column within the table; may not be null
   * @return the cast expression, or an empty string; never null
   */
  protected String valueTypeCast(TableDefinition tableDefn, ColumnId columnId) {
    if (tableDefn != null) {
      ColumnDefinition defn = tableDefn.definitionForColumn(columnId.name());
      if (defn != null) {
        String typeName = defn.typeName(); // database-specific
        if (typeName != null) {
          typeName = typeName.toLowerCase();
          if (CAST_TYPES.contains(typeName)) {
            return "::" + typeName;
          }
        }
      }
    }
    return "";
  }

  @Override
  protected int decimalScale(ColumnDefinition defn) {
    if (defn.scale() == NUMERIC_TYPE_SCALE_UNSET) {
      return NUMERIC_TYPE_SCALE_HIGH;
    }

    // Kinetica requires DECIMAL/NUMERIC columns to have a precision greater than zero
    // If the precision appears to be zero, it's because the user didn't define a fixed precision
    // for the column.
    if (defn.precision() == 0) {
      // In that case, a scale of zero indicates that there also isn't a fixed scale defined for
      // the column. Instead of treating that column as if its scale is actually zero (which can
      // cause issues since it may contain values that aren't possible with a scale of zero, like
      // 12.12), we fall back on NUMERIC_TYPE_SCALE_HIGH to try to avoid loss of precision
      if (defn.scale() == 0) {
        log.debug(
                "Column {} does not appear to have a fixed scale defined; defaulting to {}",
                defn.id(),
                NUMERIC_TYPE_SCALE_HIGH
        );
        return NUMERIC_TYPE_SCALE_HIGH;
      } else {
        // Should never happen, but if it does may signal an edge case
        // that we need to add new logic for
        log.warn(
                "Column {} has a precision of zero, but a non-zero scale of {}",
                defn.id(),
                defn.scale()
        );
      }
    }

    return defn.scale();
  }

}





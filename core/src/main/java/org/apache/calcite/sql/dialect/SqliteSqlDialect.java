package org.apache.calcite.sql.dialect;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A <code>SqlDialect</code> implementation for the SQLite database.
 */
public class SqliteSqlDialect extends SqlDialect {

    public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(DatabaseProduct.SQLITE)
      .withCaseSensitive(true)
      .withIdentifierQuoteString("\"");

    public static final SqlDialect DEFAULT =
            new SqliteSqlDialect(DEFAULT_CONTEXT);
    public SqliteSqlDialect(Context context) {
        super(context);
    }

    @Override protected boolean allowsAs() {
        return true;
    }

    @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
        SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }

    @Override public JoinType emulateJoinTypeForCrossJoin() {
        return JoinType.CROSS;
    }


}

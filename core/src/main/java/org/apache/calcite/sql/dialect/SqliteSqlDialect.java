package org.apache.calcite.sql.dialect;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;

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

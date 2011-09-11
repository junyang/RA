package ra;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.io.PrintStream;

public class DB {

    public class TableSchema {
        protected String _tableName;
        protected ArrayList<String> _colNames;
        protected ArrayList<String> _colTypes;
        public TableSchema(String tableName, ArrayList<String> colNames, ArrayList<String> colTypes) {
            _tableName = tableName;
            _colNames = colNames;
            _colTypes = colTypes;
        }
        public String getTableName() {
            return _tableName;
        }
        public ArrayList<String> getColNames() {
            return _colNames;
        }
        public ArrayList<String> getColTypes() {
            return _colTypes;
        }
        public String toPrintString() {
            String s = _tableName;
            s += "(";
            for (int i=0; i<_colNames.size(); i++) {
                if (i>0) s+= ", ";
                s += _colNames.get(i);
                s += " ";
                s += _colTypes.get(i);
            }
            s += ")";
            return s;
        }
    }

    public static void printSQLExceptionDetails(SQLException sqle, PrintStream err, boolean verbose) {
        while (sqle != null) {
            System.out.println("Error message: " + sqle.getMessage());
            System.out.println("Error code: " + sqle.getErrorCode());
            System.out.println("SQL state: " + sqle.getSQLState());
            sqle = sqle.getNextException();
        }
        return;
    }

    protected Connection _conn = null;
    protected String _driverName = null;
    protected String _schema = null;

    static ArrayList<String> loadedDriverNames = new ArrayList<String>();
    static List<String> supportedDriverNames = Arrays.asList(
            "org.sqlite.JDBC",
            "org.postgresql.Driver",
            "com.mysql.jdbc.Driver",
            "com.ibm.db2.jcc.DB2Driver"
        );
    static {
        for (String driverName : supportedDriverNames) {
            try {
                Class.forName(driverName);
                loadedDriverNames.add(driverName);
            } catch (ClassNotFoundException e) {
                // Silently ignore and move on to another driver.
            }
        }
        if (loadedDriverNames.isEmpty()) {
            System.err.println("No JDBC driver found; tried the following:");
            for (String driverName : supportedDriverNames) {
                 System.err.println(driverName);
            }
            System.exit(1);
        }
    }

    public DB(String connURL, Properties connProperties)
        throws Exception {
        _conn = DriverManager.getConnection(connURL, connProperties);
        _driverName = DriverManager.getDriver(connURL).getClass().getName();
        _schema = connProperties.getProperty("schema");
    }

    public String getDriverName() {
        return _driverName;
    }

    public void close()
        throws SQLException {
        _conn.close();
        _conn = null;
        _driverName = null;
        _schema = null;
        return;
    }

    public void execCommands(PrintStream out, String commands)
        throws SQLException {
        Statement s = _conn.createStatement();
        int resultNum = 0;
        while (true) {
            resultNum++;
            boolean queryResult;
            try {
                if (resultNum == 1) {
                    queryResult = s.execute(commands);
                } else {
                    queryResult = s.getMoreResults();
                }
            } catch (SQLException e) {
                out.println("*** Result " + resultNum + " is an error: " + e.getMessage());
                /*
                 * JDBC API for handling multiple results is a mess.
                 * 
                 * For SQL Server
                 * (http://blogs.msdn.com/b/jdbcteam/archive/2008/08/01/use-execute-and-getmoreresults-methods-for-those-pesky-complex-sql-queries.aspx),
                 * apparently when an exception is thrown here, it may
                 * just be that the current statement produced an
                 * error but subsequent statements might have
                 * succeeded, so we have to continue processing other
                 * results, with "continue;" here.  If we do "break;"
                 * instead, we will miss reporting some results.
                 * 
                 * PostgreSQL apparently supports multiple statements
                 * in one execute(), but any error will abort/rollback
                 * everything, and then getMoreResults() will
                 * subsequently return false and getUpdateCount() -1.
                 * So either "continue;" or "break;" is okay here.
                 * 
                 * SQLite apparently doesn't support multiple
                 * statements in one execute(); it just executes the
                 * first one and silently ignores others.
                 * Furthermore, if the statement fails to execute,
                 * getMoreResults() will throw an exception instead of
                 * returning false.  So we must use "break;" here.
                 * 
                 * Overall, let's just go with "break;" and document
                 * this behavior.
                 */
                break;
            }
            if (queryResult) {
                out.println("*** Result " + resultNum + " is a table:");
                ResultSet rs = s.getResultSet();
                printResultSet(out, rs);
                rs.close();
            } else {
                int rowsAffected = s.getUpdateCount();
                if (rowsAffected == -1) {
                    resultNum--;
                    break;
                }
                out.println("*** Result " + resultNum + " is an update count of " + rowsAffected);
            }
        }
        s.close();
        return;
    }

    public void printResultSet(PrintStream out, ResultSet rs)
        throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        // Print result heading:
        out.print("Output schema: (");
        int numCols = rsmd.getColumnCount();
        for (int i=1; i<=numCols; i++) {
            if (i>1) out.print(", ");
            out.print(rsmd.getColumnName(i) + " " + rsmd.getColumnTypeName(i));
        }
        out.println(")");
        out.println("-----");
        // Print result content:
        int count = 0;
        while (rs.next()) {
            for (int i=1; i<=numCols; i++) {
                if (i>1) out.print("|");
                String colString;
                switch (rsmd.getColumnType(i)) {
                case Types.INTEGER:
                    colString = String.valueOf(rs.getInt(i));
                    break;
                case Types.SMALLINT:
                    colString = String.valueOf(rs.getShort(i));
                    break;
                case Types.DOUBLE:
                    colString = String.valueOf(rs.getDouble(i));
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    colString = String.valueOf(rs.getFloat(i));
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    colString = rs.getBigDecimal(i).toString();
                    break;
                case Types.DATE:
                    colString = rs.getDate(i).toString();
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                    colString = rs.getString(i);
                    break;
                default:
                    colString = null;
                    break;
                }
                if (colString == null) {
                    colString = "<TYPE UNSUPPORTED>";
                } else if (rs.wasNull()) {
                    colString = "<NULL>";
                }
                out.print(colString);
            }
            out.println();
            count++;
        }
        // Print result summary:
        out.println("-----");
        out.println("Total number of rows: " + count);
        out.println();
        return;
    }

    public void execQueryAndOutputResult(PrintStream out, String query)
        throws SQLException {
        Statement s = _conn.createStatement();
        ResultSet rs = s.executeQuery(query);
        printResultSet(out, rs);
        rs.close();
        s.close();
        return;
    }

    public ArrayList<String> getTables()
        throws SQLException {
        ArrayList<String> tableNames = new ArrayList<String>();
        DatabaseMetaData dbmd = _conn.getMetaData();
        ResultSet rs = dbmd.getTables(null, _schema, null, new String[] { "TABLE", "VIEW" });
        while (rs.next()) {
            String tableName = rs.getString(3);
            tableNames.add(tableName);
        }
        rs.close();
        return tableNames;
    }

    public TableSchema getOutputSchema(String query)
        throws SQLException {
        ArrayList<String> colNames = new ArrayList<String>();
        ArrayList<String> colTypes = new ArrayList<String>();
        Statement s = _conn.createStatement();
        ResultSet rs = s.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int numCols = rsmd.getColumnCount();
        for (int i=1; i<=numCols; i++) {
            // Important: Use getColumnLabel() to get new column names specified
            // in AS or in CREATE VIEW.  For some JDBC drivers, getColumnName()
            // gives the original column names inside base tables.
            colNames.add(rsmd.getColumnLabel(i));
            colTypes.add(rsmd.getColumnTypeName(i));
        }
        rs.close();
        return new TableSchema(null, colNames, colTypes);
    }

    public TableSchema getTableSchema(String tableName)
        throws SQLException {
        TableSchema schema = getOutputSchema("SELECT * FROM " + tableName);
        return new TableSchema(tableName, schema.getColNames(), schema.getColTypes());
    }

    // The following implementation does not seem to work for
    // postgresql when tableName is a view:
    //
    // public TableSchema getTableSchema(String tableName)
    //     throws SQLException {
    //     ArrayList<String> colNames = new ArrayList<String>();
    //     ArrayList<String> colTypes = new ArrayList<String>();
    //     DatabaseMetaData dbmetadta = _conn.getMetaData();
    //     ResultSet rs = dbmetadta.getColumns(null, null, tableName, null);
    //     while (rs.next()) {
    //         String colName = rs.getString(4);
    //         String colType = rs.getString(6);
    //         colNames.add(colName);
    //         colTypes.add(colType);
    //     }
    //     rs.close();
    //     if (colNames.isEmpty()) // No such table!
    //         return null;
    //     return new TableSchema(tableName, colNames, colTypes);
    // }

    public void createView(String createViewStatement)
        throws SQLException {
        Statement s = _conn.createStatement();
        s.executeUpdate(createViewStatement);
        s.close();
        return;
    }

    public void dropView(String viewName)
        throws SQLException {
        Statement s = _conn.createStatement();
        s.executeUpdate("DROP VIEW " + viewName);
        s.close();
        return;
    }

}

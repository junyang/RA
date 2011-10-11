package ra;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.PrintStream;
import java.sql.SQLException;

public abstract class RAXNode {

    protected static int _viewGeneratedCount = 0;
    public static String generateViewName() {
        _viewGeneratedCount++;
        return "RA_TMP_VIEW_" + _viewGeneratedCount;
    }
    public static void resetViewNameGenerator() {
        _viewGeneratedCount = 0;
    }

    public enum Status { ERROR, UNCHECKED, CORRECT }

    protected Status _status;
    protected String _viewName;
    protected DB.TableSchema _outputSchema;
    protected ArrayList<RAXNode> _children;
    protected RAXNode(ArrayList<RAXNode> children) {
        _status = Status.UNCHECKED;
        _viewName = generateViewName();
        _outputSchema = null;
        _children = children;
    }
    public String getViewName() {
        return _viewName;
    }
    public int getNumChildren() {
        return _children.size();
    }
    public RAXNode getChild(int i) {
        return _children.get(i);
    }
    public abstract String genViewDef(DB db)
        throws SQLException, ValidateException;
    public String genViewCreateStatement(DB db)
        throws SQLException, ValidateException {
        return "CREATE VIEW " + _viewName + " AS " + genViewDef(db);
    }
    public abstract String toPrintString();
    public void print(boolean verbose, int indent, PrintStream out) {
        for (int i=0; i<indent; i++) out.print(" ");
        out.print(toPrintString());
        if (verbose) {
            if (_status == Status.CORRECT) {
                out.print(" <- output schema: " + _outputSchema.toPrintString());
            } else if (_status == Status.ERROR) {
                out.print(" <- ERROR!");
            }
        }
        out.println();
        for (int i=0; i<getNumChildren(); i++) {
            getChild(i).print(verbose, indent+4, out);
        }
        return;
    }
    public void validate(DB db)
        throws ValidateException {
        // Validate children first; any exception thrown there
        // will shortcut the call.
        for (int i=0; i<getNumChildren(); i++) {
            getChild(i).validate(db);
        }
        try {
            // Drop the view, just in case it is left over from
            // a previous run (shouldn't have happened if it was
            // a clean run):
            db.dropView(_viewName);
        } catch (SQLException e) {
            // Simply ignore; this is probably not safe.  I would
            // imagine that we are trying to drop view8 as the root
            // view, but in a previous run view8 is used to define
            // view9, so view8 cannot be dropped before view9.  A
            // robust solution seems nasty.
        }
        try {
            db.createView(genViewCreateStatement(db));
            _outputSchema = db.getTableSchema(_viewName);
            assert(_outputSchema != null);
        } catch (SQLException e) {
            _status = Status.ERROR;
            // Wrap and re-throw the exception for caller to handle.
            throw new ValidateException(e, this);
        }
        // Everything rooted at this node went smoothly.
        _status = Status.CORRECT;
        return;
    }
    public void execute(DB db, PrintStream out)
        throws SQLException {
        assert(_status == Status.CORRECT);
        db.execQueryAndOutputResult(out, "SELECT * FROM " + _viewName);
        return;
    }
    public void clean(DB db) 
        throws SQLException {
        if (_status == Status.UNCHECKED) {
            // Should be the case that the view wasn't actually created.
        } else if (_status == Status.CORRECT) {
            db.dropView(_viewName);
            _status = Status.UNCHECKED;
        } else if (_status == Status.ERROR) {
            // The view shouldn't have been created successfully; no
            // need to drop.
            _status = Status.UNCHECKED;
        }
        for (int i=0; i<getNumChildren(); i++) {
            getChild(i).clean(db);
        }
        return;
    }

    public static class ValidateException extends Exception {
        protected SQLException _sqlException;
        protected RAXNode _errorNode;
        public ValidateException(SQLException sqlException, RAXNode errorNode) {
            _sqlException = sqlException;
            _errorNode = errorNode;
        }
        public ValidateException(String message, RAXNode errorNode) {
            super(message);
            _sqlException = null;
            _errorNode = errorNode;
        }
        public SQLException getSQLException() {
            return _sqlException;
        }
        public RAXNode getErrorNode() {
            return _errorNode;
        }
    }

    public static class TABLE extends RAXNode {
        protected String _tableName;
        public TABLE(String tableName) {
            super(new ArrayList<RAXNode>());
            _tableName = tableName;
        }
        public String genViewDef(DB db)
            throws SQLException {
            return "SELECT DISTINCT * FROM " + _tableName;
        }
        public String toPrintString() {
            return _tableName;
        }
    }

    public static class SELECT extends RAXNode {
        protected String _condition;
        public SELECT(String condition, RAXNode input) {
            super(new ArrayList<RAXNode>(Arrays.asList(input)));
            _condition = condition;
        }
        public String genViewDef(DB db)
            throws SQLException {
            return "SELECT * FROM " + getChild(0).getViewName() +
                " WHERE " + _condition;
        }
        public String toPrintString() {
            return "\\select_{" + _condition + "}";
        }
    }

    public static class PROJECT extends RAXNode {
        protected String _columns;
        public PROJECT(String columns, RAXNode input) {
            super(new ArrayList<RAXNode>(Arrays.asList(input)));
            _columns = columns;
        }
        public String genViewDef(DB db)
            throws SQLException {
            return "SELECT DISTINCT " + _columns + " FROM " + getChild(0).getViewName();
        }
        public String toPrintString() {
            return "\\project_{" + _columns + "}";
        }
    }

    public static class JOIN extends RAXNode {
        protected String _condition;
        public JOIN(String condition, RAXNode input1, RAXNode input2) {
            super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
            _condition = condition;
        }
        public String genViewDef(DB db)
            throws SQLException {
            if (_condition == null) {
                // Natural join:
                DB.TableSchema input1Schema = db.getTableSchema(getChild(0).getViewName());
                DB.TableSchema input2Schema = db.getTableSchema(getChild(1).getViewName());
                List<String> input1ColumnNames = input1Schema.getColNames();
                List<String> input2ColumnNames = input2Schema.getColNames();
                List<String> joinColumnNames = new ArrayList<String>();
                List<String> moreColumnNames = new ArrayList<String>();
                for (String col : input2ColumnNames) {
                    if (input1ColumnNames.contains(col)) {
                        joinColumnNames.add(col);
                    } else {
                        moreColumnNames.add(col);
                    }
                }
                if (joinColumnNames.isEmpty()) {
                    // Basically a cross product:
                    return "SELECT * FROM " +
                        getChild(0).getViewName() + ", " + getChild(1).getViewName();
                } else {
                    String viewDef = "SELECT ";
                    for (int i=0; i<input1ColumnNames.size(); i++) {
                        if (i > 0) viewDef += ", ";
                        viewDef += "V1.\"" + input1ColumnNames.get(i) + "\"";
                    }
                    for (String col : moreColumnNames) {
                        viewDef += ", V2.\"" + col + "\"";
                    }
                    viewDef += " FROM " +
                        getChild(0).getViewName() + " AS V1, " +
                        getChild(1).getViewName() + " AS V2 WHERE ";
                    for (int i=0; i<joinColumnNames.size(); i++) {
                        if (i > 0) viewDef += " AND ";
                        viewDef += "V1.\"" + joinColumnNames.get(i) +
                            "\"=V2.\"" + joinColumnNames.get(i) + "\"";
                    }
                    return viewDef;
                }
            } else {
                // Theta-join:
                return "SELECT * FROM " +
                    getChild(0).getViewName() + ", " + getChild(1).getViewName() +
                    " WHERE " + _condition;
            }
        }
        public String toPrintString() {
            return "\\join_{" + _condition + "}";
        }
    }

    public static class CROSS extends RAXNode {
        public CROSS(RAXNode input1, RAXNode input2) {
            super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
        }
        public String genViewDef(DB db)
            throws SQLException {
            return "SELECT * FROM " +
                getChild(0).getViewName() + ", " + getChild(1).getViewName();
        }
        public String toPrintString() {
            return "\\cross";
        }
    }

    public static class UNION extends RAXNode {
        public UNION(RAXNode input1, RAXNode input2) {
            super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
        }
        public String genViewDef(DB db)
            throws SQLException {
            return "SELECT * FROM " + getChild(0).getViewName() +
                " UNION SELECT * FROM " + getChild(1).getViewName();
        }
        public String toPrintString() {
            return "\\union";
        }
    }

    public static class DIFF extends RAXNode {
        public DIFF(RAXNode input1, RAXNode input2) {
            super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
        }
        public String genViewDef(DB db)
            throws SQLException, ValidateException {
            if (db.getDriverName().equals("com.mysql.jdbc.Driver")) {
                // MySQL doesn't support EXCEPT, so we need a workaround.
                // First, get the input schema of the children, which
                // should have already been validated so their views
                // have been created at this point:
                DB.TableSchema input1Schema = db.getTableSchema(getChild(0).getViewName());
                DB.TableSchema input2Schema = db.getTableSchema(getChild(1).getViewName());
                if (input1Schema.getColNames().size() !=
                    input2Schema.getColNames().size()) {
                    throw new ValidateException("taking the difference between relations with different numbers of columns", this);
                }
                String viewDef = "SELECT * FROM " + getChild(0).getViewName() +
                    " WHERE NOT EXISTS (SELECT * FROM " + getChild(1).getViewName() +
                    " WHERE ";
                for (int i=0; i<input1Schema.getColNames().size(); i++) {
                    if (i>0) viewDef += " AND ";
                    viewDef += getChild(0).getViewName() + ".\"" +
                        input1Schema.getColNames().get(i) + "\"=" +
                        getChild(1).getViewName() + ".\"" +
                        input2Schema.getColNames().get(i) + "\"";
                }
                viewDef += ")";
                return viewDef;
            } else {
                return "SELECT * FROM " + getChild(0).getViewName() +
                    " EXCEPT SELECT * FROM " + getChild(1).getViewName();
            }
        }
        public String toPrintString() {
            return "\\diff";
        }
    }

    public static class INTERSECT extends RAXNode {
        public INTERSECT(RAXNode input1, RAXNode input2) {
            super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
        }
        public String genViewDef(DB db)
            throws SQLException, ValidateException {
            if (db.getDriverName().equals("com.mysql.jdbc.Driver")) {
                // MySQL doesn't support INTERSECT, so we need a workaround.
                // First, get the input schema of the children, which
                // should have already been validated so their views
                // have been created at this point:
                DB.TableSchema input1Schema = db.getTableSchema(getChild(0).getViewName());
                DB.TableSchema input2Schema = db.getTableSchema(getChild(1).getViewName());
                if (input1Schema.getColNames().size() !=
                    input2Schema.getColNames().size()) {
                    throw new ValidateException("intersecting relations with different numbers of columns", this);
                }
                String viewDef = "SELECT DISTINCT * FROM " + getChild(0).getViewName() +
                    " WHERE EXISTS (SELECT * FROM " + getChild(1).getViewName() +
                    " WHERE ";
                for (int i=0; i<input1Schema.getColNames().size(); i++) {
                    if (i>0) viewDef += " AND ";
                    viewDef += getChild(0).getViewName() + ".\"" +
                        input1Schema.getColNames().get(i) + "\"=" +
                        getChild(1).getViewName() + ".\"" +
                        input2Schema.getColNames().get(i) + "\"";
                }
                viewDef += ")";
                return viewDef;
            } else {
                return "SELECT * FROM " + getChild(0).getViewName() +
                    " INTERSECT SELECT * FROM " + getChild(1).getViewName();
            }
        }
        public String toPrintString() {
            return "\\intersect";
        }
    }

    public static class RENAME extends RAXNode {
        protected String _columns;
        public RENAME(String columns, RAXNode input) {
            super(new ArrayList<RAXNode>(Arrays.asList(input)));
            _columns = columns;
        }
        public String genViewDef(DB db)
            throws SQLException, ValidateException {
            if (db.getDriverName().equals("org.sqlite.JDBC")) {
                // SQLite doesn't allows view column names to be
                // specified, so we have to dissect the list of new
                // column names and build the SELECT clause.
                // First, get the input schema of the child, which
                // should have already been validated so its view 
                // has been created at this point:
                DB.TableSchema inputSchema = db.getTableSchema(getChild(0).getViewName());
                // Next, parse the list of new column names:
                List<String> columnNames = parseColumnNames(_columns);
                if (inputSchema.getColNames().size() != columnNames.size()) {
                    throw new ValidateException("renaming an incorrect number of columns", this);
                }
                String viewDef = "SELECT ";
                for (int i=0; i<columnNames.size(); i++) {
                    if (i>0) viewDef += ", ";
                    viewDef += "\"" + inputSchema.getColNames().get(i) +
                        "\" AS " + columnNames.get(i);
                }
                viewDef += " FROM " + getChild(0).getViewName();
                return viewDef;
            } else {
                return "SELECT * FROM " + getChild(0).getViewName();
            }
        }
        public String genViewCreateStatement(DB db)
            throws SQLException, ValidateException {
            if (db.getDriverName().equals("org.sqlite.JDBC")) {
                // See comments in genViewDef(DB):
                return "CREATE VIEW " + _viewName + " AS " + genViewDef(db);
            } else {
                return "CREATE VIEW " + _viewName + "(" + _columns + ") AS " +
                    genViewDef(db);
            }
        }
        public String toPrintString() {
            return "\\rename_{" + _columns + "}";
        }
    }

    public static List<String> parseColumnNames(String columns) {
        String[] columnNames = columns.split("\\s*,\\s*");
        return Arrays.asList(columnNames);
    }
}

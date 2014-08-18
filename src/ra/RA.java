package ra;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.sql.*;
import jargs.gnu.CmdLineParser;
import antlr.RecognitionException;
import antlr.TokenStreamException;
import antlr.CommonAST;
import antlr.collections.AST;
import jline.console.ConsoleReader;
import jline.console.completer.StringsCompleter;

public class RA {

    protected static PrintStream out = System.out;
    protected static PrintStream err = System.err;
    protected static InputStream in = null;
    protected static ConsoleReader reader = null;
    protected static DB db = null;

    protected static void exit(int code) {
        try {
            if (db != null) db.close();
        } catch (SQLException e) {
            // Simply ignore.
        }
        System.exit(code);
    }

    protected static void welcome() {
        out.println();
        out.println("RA: an interactive relational algebra interpreter");
        out.println("Version " + RA.class.getPackage().getImplementationVersion() +
                    " by Jun Yang (junyang@cs.duke.edu)");
        out.println("http://www.cs.duke.edu/~junyang/ra/");
        out.println("Type \"\\help;\" for help");
        out.println();
        return;
    }

    protected static void usage() {
        out.println("Usage: ra [Options] [PROPS_FILE]");
        out.println("Options:");
        out.println("  -h: print this message, and exit");
        out.println("  -i FILE: read commands from FILE instead of standard input");
        out.println("  -o FILE: save a transcript of the session in FILE");
        out.println("  -v: turn on verbose output");
        out.println("  -l URL: use URL for JDBC database connection");
        out.println("    (overriding the URL in PROPS_FILE)");
        out.println("  -p PASSWD: use PASSWD to connect to the database");
        out.println("    (overriding any password in PROPS_FILE)");
        out.println("  -P: prompt for database password");
        out.println("    (overriding any password in PROPS_FILE)");
        out.println("  -u USER: connect to the database as USER");
        out.println("    (overriding any user in PROPS_FILE)");
        out.println("PROPS_FILE: specifies the JDBC connection URL and properties");
        out.println("    (defaults to /ra/ra.properties packaged in ra.jar)");
        out.println();
        return;
    }

    protected static void prompt(int line) {
        if (reader == null) return;
        reader.setPrompt((line == 1)? "ra> " : "" + line + "> ");
        return;
    }

    protected static void exit() {
        out.println("Bye!");
        out.println();
        exit(0);
        return;
    }

    protected static String getPassword(ConsoleReader reader) {
        String password = null;
        try {
            password = reader.readLine("Password: ", new Character((char)0));
        } catch (IOException e) {
        }
        if (password == null) {
            err.println("Error reading password input");
            err.println();
            exit(1);
        }
        return password;
    }

    protected static void skipInput() {
        try {
            (new BufferedReader(new InputStreamReader(in))).readLine();
        } catch (IOException e) {
            err.println("Unexceptected I/O error:");
            err.println(e.toString());
            err.println();
            exit(1);
        }
    }

    public static void main(String[] args) {

        welcome();
        CmdLineParser cmdLineParser = new CmdLineParser();
        CmdLineParser.Option helpO = cmdLineParser.addBooleanOption('h', "help");
        CmdLineParser.Option inputO = cmdLineParser.addStringOption('i', "input");
        CmdLineParser.Option outputO = cmdLineParser.addStringOption('o', "output");
        CmdLineParser.Option passwordO = cmdLineParser.addStringOption('p', "password");
        CmdLineParser.Option promptPasswordO = cmdLineParser.addBooleanOption('P', "prompt-password");
        CmdLineParser.Option schemaO = cmdLineParser.addStringOption('s', "schema");
        CmdLineParser.Option urlO = cmdLineParser.addStringOption('l', "url");
        CmdLineParser.Option userO = cmdLineParser.addStringOption('u', "user");
        CmdLineParser.Option verboseO = cmdLineParser.addBooleanOption('v', "verbose");
        try {
            cmdLineParser.parse(args);
        } catch (CmdLineParser.OptionException e) {
            err.println(e.getMessage());
            usage();
            exit(1);
        }
        boolean help = ((Boolean)cmdLineParser.getOptionValue(helpO, Boolean.FALSE)).booleanValue();
        String inFileName = (String)cmdLineParser.getOptionValue(inputO);
        String outFileName = (String)cmdLineParser.getOptionValue(outputO);
        String password = (String)cmdLineParser.getOptionValue(passwordO);
        boolean promptPassword = ((Boolean)cmdLineParser.getOptionValue(promptPasswordO, Boolean.FALSE)).booleanValue();
        String schema = (String)cmdLineParser.getOptionValue(schemaO);
        String url = (String)cmdLineParser.getOptionValue(urlO);
        String user = (String)cmdLineParser.getOptionValue(userO);
        boolean verbose = ((Boolean)cmdLineParser.getOptionValue(verboseO, Boolean.FALSE)).booleanValue();
        if (help) {
            usage();
            exit(1);
        }
        String propsFileName = null;
        String[] otherArgs = cmdLineParser.getRemainingArgs();
        if (otherArgs.length > 1) {
            usage();
            exit(1);
        } else if (otherArgs.length == 1) {
            propsFileName = otherArgs[0];
        }
        if (inFileName != null) {
            try {
                in = new FileInputStream(inFileName);
            } catch (FileNotFoundException e) {
                err.println("Error opening input file '" + inFileName + "'");
                err.println();
                exit(1);
            }
        } else {
            try {
                reader = new ConsoleReader();
                // Make sure ConsoleReader doesn't do funny things with backslashes:
                reader.setExpandEvents(false);
                in = new ConsoleReaderInputStream(reader);
            } catch (IOException e) {
                err.println("Unexceptected I/O error:");
                err.println(e.toString());
                err.println();
                exit(1);
            }
        }
        if (outFileName != null) {
            try {
                OutputStream log = new FileOutputStream(outFileName, true);
                out = new TeePrintStream(out, log);
                err = new TeePrintStream(err, log);
                in = new LogInputStream(in, log);
            } catch (FileNotFoundException e) {
                err.println("Error opening output file '" + outFileName + "'");
                err.println();
                exit(1);
            }
        }
        Properties props = new Properties();
        InputStream propsIn = null;
        if (propsFileName == null) {
            propsIn = RA.class.getResourceAsStream("ra.properties");
            if (propsIn == null) {
                err.println("Error loading properties from /ra/ra.properties in the jar file");
                exit(1);
            }
            try {
                props.load(propsIn);
            } catch (IOException e) {
                err.println("Error loading properties from /ra/ra.properties in the jar file");
                err.println(e.toString());
                err.println();
                exit(1);
            }
        } else {
            try {
                props.load(new FileInputStream(propsFileName));
            } catch (IOException e) {
                err.println("Error loading properties from " + propsFileName);
                err.println(e.toString());
                err.println();
                exit(1);
            }
        }
        if (url != null)
            props.setProperty("url", url);
        if (user != null)
            props.setProperty("user", user);
        if (password != null)
            props.setProperty("password", password);
        if (promptPassword) {
            try {
                props.setProperty("password",
                                  getPassword((reader == null)?
                                              new ConsoleReader() :
                                              reader));
            } catch (IOException e) {
                err.println("Unexceptected I/O error:");
                err.println(e.toString());
                err.println();
                exit(1);
            }
        }
        try {
            db = new DB(props.getProperty("url"), props);
        } catch (Exception e) {
            err.println("Error connecting to the database");
            err.println(e.toString());
            err.println();
            exit(1);
        }
        if (schema != null)
            props.setProperty("schema", schema);

        if (reader != null) {
            reader.addCompleter(new StringsCompleter(new String [] {
                "\\help;", "\\quit;", "\\list;", "\\sqlexec_{",
                "\\select_{", "\\project_{", "\\join", "\\join_{", "\\rename_{",
                "\\cross", "\\union", "\\diff", "\\intersect"
            }));
        }

        DataInputStream din = new DataInputStream(in);
        while (true) {
            // Clean start every time.
            prompt(1);
            RALexer lexer = new RALexer(din);
            RAParser parser = new RAParser(lexer);
            try {
                parser.start();
                CommonAST ast = (CommonAST)parser.getAST();
                evaluate(verbose, db, ast);
            } catch (TokenStreamException e) {
                skipInput();
                err.println("Error tokenizing input:");
                err.println(e.toString());
                err.println("Rest of input skipped");
                err.println();
                continue;
            } catch (RecognitionException e) {
                skipInput();
                err.println("Error parsing input:");
                err.println(e.toString());
                err.println("Rest of input skipped");
                err.println();
                continue;
            }
        }
    }

    protected static void evaluate(boolean verbose, DB db, CommonAST ast) {
        if (ast.getType() == RALexerTokenTypes.QUIT ||
            ast.getType() == RALexerTokenTypes.EOF) {
            exit();
        } else if (ast.getType() == RALexerTokenTypes.HELP) {
            out.println("Terminate your commands or expressions by \";\"");
            out.println();
            out.println("Commands:");
            out.println("\\help: print this message");
            out.println("\\quit: exit ra");
            out.println("\\list: list all relations in the database");
            out.println("\\sqlexec_{STATEMENT}: execute SQL in the database");
            out.println();
            out.println("Relational algebra expressions:");
            out.println("R: relation named by R");
            out.println("\\select_{COND} EXP: selection over an expression");
            out.println("\\project_{ATTR_LIST} EXP: projection of an expression");
            out.println("EXP_1 \\join EXP_2: natural join between two expressions");
            out.println("EXP_1 \\join_{COND} EXP_2: theta-join between two expressions");
            out.println("EXP_1 \\cross EXP_2: cross-product between two expressions");
            out.println("EXP_1 \\union EXP_2: union between two expressions");
            out.println("EXP_1 \\diff EXP_2: difference between two expressions");
            out.println("EXP_1 \\intersect EXP_2: intersection between two expressions");
            out.println("\\rename_{NEW_ATTR_NAME_LIST} EXP: rename all attributes of an expression");
            out.println();
        } else if (ast.getType() == RALexerTokenTypes.LIST) {
            try {
                ArrayList<String> tables = db.getTables();
                out.println("-----");
                for (int i=0; i<tables.size(); i++) {
                    out.println(tables.get(i));
                }
                out.println("-----");
                out.println("Total of " + tables.size() + " table(s) found.");
                out.println();
            } catch (SQLException e) {
                err.println("Unexpected error obtaining list of tables from database");
                db.printSQLExceptionDetails(e, err, verbose);
                err.println();
            }
        } else if (ast.getType() == RALexerTokenTypes.SQLEXEC) {
            try {
                assert(ast.getFirstChild().getType() == RALexerTokenTypes.OPERATOR_OPTION);
                String sqlCommands = ast.getFirstChild().getText();
                db.execCommands(out, sqlCommands);
            } catch (SQLException e) {
                err.println("Error executing SQL commands");
                db.printSQLExceptionDetails(e, err, verbose);
                err.println();
            }
        } else {
            RAXNode rax = null;
            try {
                RAXConstructor constructor = new RAXConstructor();
                RAXNode.resetViewNameGenerator();
                rax = constructor.expr(ast);
                if (verbose) {
                    out.println("Parsed query:");
                    rax.print(verbose, 0, out);
                    out.println("=====");
                }
                rax.validate(db);
                if (verbose) {
                    out.println("Validated query:");
                    rax.print(verbose, 0, out);
                    out.println("=====");
                }
                rax.execute(db, out);
            } catch (RecognitionException e) {
                // From constructor.expr():
                err.println("Unexpected error constructing queries from parse tree:");
                err.println(e.toString());
                err.println();
            } catch (RAXNode.ValidateException e) {
                // From rax.validate():
                err.println("Error validating subquery:");
                e.getErrorNode().print(true, 0, err);
                if (e.getMessage() != null) {
                    err.println(e.getMessage());
                }
                if (e.getSQLException() != null) {
                    db.printSQLExceptionDetails(e.getSQLException(), err, verbose);
                }
                err.println();
            } catch (SQLException e) {
                // From rax.execute():
                err.println("Unexpected error executing validated query:");
                db.printSQLExceptionDetails(e, err, verbose);
                err.println();
            }
            // Remember to clean up the views created by rax:
            try {
                if (rax != null) rax.clean(db);
            } catch (SQLException e) {
                err.println("Unexpected error cleaning up query");
                db.printSQLExceptionDetails(e, err, verbose);
                err.println();
            }
        }
        return;
    }
}

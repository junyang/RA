header {
    package ra;
    import antlr.CommonAST;
}

//////////////////////////////////////////////////////////////////////

class RALexer extends Lexer;

options {
    // Lookahead needs to be bigger than usual.
    k = 3;
    defaultErrorHandler = false;
}

protected DIGIT: '0'..'9';
protected ALPHA: ('a'..'z'|'A'..'Z');
protected NEWLINE
    : (options {
           // To handle dos/mac/unix newlines, the resulting grammer
           // is necessarily ambiguous.
           generateAmbigWarnings = false;
       } : "\r\n" // DOS.
           | '\r' // Mac.
           | '\n' // Unix.
           // The ordering above is important, as it gives "\r\n" the
           // highest priority in producing a match.
      ) {
// Another way around this newline mess, but apparently this is not as
// robust as above because input file saved on one platform would not
// work on anther.
//     : ({System.getProperty("line.separator").equals("\r")}? '\r' |
//        {System.getProperty("line.separator").equals("\n")}? '\n' |
//        {System.getProperty("line.separator").equals("\r\n")}? "\r\n") {
            newline();
            RA.prompt(getLine());
        }
    ;
protected WHITE_SPACE_NO_NEWLINE : ' '|'\t'|'\f';
WHITE_SPACE
    : (WHITE_SPACE_NO_NEWLINE|NEWLINE)+ {
            $setType(Token.SKIP); // Ignore.
        }
    ;
COMMENT
    : "//" (~('\n'|'\r'))* {
            $setType(Token.SKIP); // Ignore.
        }
    | "/*" (
          options {
              // Turn off greedy option, so that the "*" in "*/"
              // will not be consumed by INSIDE_MULTILINE_COMMENT.
              greedy = false;
          }
          : INSIDE_MULTILINE_COMMENT
      )* "*/" {
            $setType(Token.SKIP); // Ignore.
        }
    ;
protected INSIDE_MULTILINE_COMMENT
    : NEWLINE
    | ~('\n'|'\r')
    ;
LEFT_PAREN : '(';
RIGHT_PAREN : ')';
STATEMENT_TERMINATOR : ';' (WHITE_SPACE_NO_NEWLINE)* NEWLINE;
TABLE_NAME : ALPHA (ALPHA|DIGIT|'_')*;
SELECT : "\\select";
PROJECT : "\\project";
JOIN : "\\join";
CROSS : "\\cross";
UNION : "\\union";
DIFF : "\\diff";
INTERSECT: "\\intersect";
RENAME : "\\rename";
SQLEXEC : "\\sqlexec";
LIST : "\\list";
HELP : "\\help";
QUIT : "\\quit";
OPERATOR_OPTION
    : "_{"! (INSIDE_OPERATOR_OPTION)* '}'!
    // Note that !'s above discard surrounding delimitors.
    ;
protected INSIDE_OPERATOR_OPTION
    : NEWLINE
    | ~('}'|'\n'|'\r')
    ;

//////////////////////////////////////////////////////////////////////

class RAParser extends Parser;

options {
    // Build the AST automatically.
    buildAST = true;
    defaultErrorHandler = false;
}
start
    : expr STATEMENT_TERMINATOR!
    // Note that !'s above prevent the token from being included in AST.
    | SQLEXEC^ OPERATOR_OPTION STATEMENT_TERMINATOR!
    // Note that ^'s above explicitly specify what the AST roots should be.
    | LIST STATEMENT_TERMINATOR!
    | HELP STATEMENT_TERMINATOR!
    | QUIT STATEMENT_TERMINATOR!
    | EOF
    ;
// The following rules attempt to let antlr parse the entire
// input, and handle error recovery by default.  However, there
// are some issues with error recovery; upon encountering an
// error, sometime antlr was unable to recovery correctly.
// start
//     : (statement)* EOF {
//             RA.exit();
//         }
//     ;
// statement
//     : expr STATEMENT_TERMINATOR! {
//             RA.evaluate((CommonAST)#statement);
//         }
//     | QUIT! STATEMENT_TERMINATOR! {
//             RA.exit();
//         }
//     ;
expr_unit
    : TABLE_NAME
    | LEFT_PAREN! expr RIGHT_PAREN!
    ;
expr_unary
    : expr_unit
    | SELECT^ OPERATOR_OPTION expr_unary
    | PROJECT^ OPERATOR_OPTION expr_unary
    | RENAME^ OPERATOR_OPTION expr_unary
    ;
expr
    : expr_unary ((JOIN^ (OPERATOR_OPTION)?|CROSS^|UNION^|DIFF^|INTERSECT^) expr_unary)*
    // Note that ^'s above explicitly specify what the AST roots should be.
    ;

//////////////////////////////////////////////////////////////////////

class RAXConstructor extends TreeParser;

options {
    defaultErrorHandler = false;
}

expr returns [RAXNode r = null] {
    RAXNode input, input1, input2;
}
    : #(JOIN input1=expr (jc:OPERATOR_OPTION)? input2=expr) {
            r = new RAXNode.JOIN((jc == null)? null : jc.getText(), input1, input2);
        }
    | #(CROSS input1=expr input2=expr) {
            r = new RAXNode.CROSS(input1, input2);
        }
    | #(UNION input1=expr input2=expr) {
            r = new RAXNode.UNION(input1, input2);
        }
    | #(DIFF input1=expr input2=expr) {
            r = new RAXNode.DIFF(input1, input2);
        }
    | #(INTERSECT input1=expr input2=expr) {
            r = new RAXNode.INTERSECT(input1, input2);
        }
    | #(SELECT sc:OPERATOR_OPTION input=expr) {
            r = new RAXNode.SELECT(sc.getText(), input);
        }
    | #(PROJECT pc:OPERATOR_OPTION input=expr) {
            r = new RAXNode.PROJECT(pc.getText(), input);
        }
    | #(RENAME rc:OPERATOR_OPTION input=expr) {
            r = new RAXNode.RENAME(rc.getText(), input);
        }
    | t:TABLE_NAME {
            r = new RAXNode.TABLE(t.getText());
        }
    ;

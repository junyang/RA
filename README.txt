* for more info, visit http://www.cs.duke.edu/~junyang/ra/

* standard usage:
  - the most convenient way to us ra is to first prepare a connection
    properties file.  see res/ra/ra.properties for an example.  if you
    want to make it default, edit res/ra/ra.properties and rebuild the
    jar file, or just edit ra/ra.properties inside the jar.
  - then, to run ra:
    java -ea -jar ra.jar # this will use the default properties in the jar
    java -ea -jar ra.jar -P # this will prompt for a password
    java -ea -jar ra.jar PROP_FILE  # this will use the supplied properties file
    java -ea -jar ra.jar PROP_FILE -vP
  - if you don't want to prepare a connection properties file, you can specify
    all connection information as options (-l URL -u USER -p PASSWD); for
    details type:
    java -jar ra.jar -h

* external dependencies:
  - you need ant, a java build tool.  the build script is build.xml.
 
* lib/ directory contains the necessary jar libraries, including antlr
  (for parsing), jline2 (for command-line editing), jargs (for parsing
  arguments), and various jdbc drivers.  to make sure that a jdbc
  driver is loaded, add a Class.forName call to src/ra/DB.java.
 
* src/ directory holds the source files.
  - ra/RA.java is the main driver.
  - ra/DB.java contains code that interfaces with the database.
  - ra/ra.g specifies the relational algebra grammer.
  - ra/RAXNode.java defines nodes for representating relational algebra
    expression trees.  it also implements the translation of these
    expression trees into sql, using structural recursion on the trees
    and one sql view definition per node.
  - ra/TeePrintStream.java is just a simple utility class that allows
    output to be tee'd into a file.

* sample.* are an example of working with a sqlite database.
  - sample.properties is the connection properties file.
  - sample.db is the database file (can be built by "ant sample.db").
  - sample.ra contains the commands for constructing/reconstructing 
    the above database.  the database file was constructed initially
    by running "ant sample.db", or:
    java -ea -jar ra.jar sample.properties -i sample.ra

package ra;

import java.io.OutputStream;
import java.io.PrintStream;

public class TeePrintStream extends PrintStream {
    protected PrintStream _dupOut;
    public TeePrintStream(PrintStream out1, OutputStream out2) {
        // Ideally, we would like to make out1 and out2 have the
        // same autoFlush option, but the API does not allow us
        // to do that.  So we assume autoFlush is true (correct
        // for System.out and System.err).
        super(out2, true);
        _dupOut = out1;
        return;
    }
    // Just override the primitives, since print and println
    // methods seem to just use them.
    public boolean checkError() {
        return super.checkError() || _dupOut.checkError();
    }
    public void close() {
        // No effect; closing should be done on the original streams
        // used in the constructor.  Not sure how safe this is.
        return;
    }
    public void flush() {
        super.flush();
        _dupOut.flush();
    }
    protected void setError() {
        // Ideally we would like to do the following, but we cannot
        // because setError is protected.
        // super.setError();
        // _dupOut.setError();
        assert(false);
        return;
    }
    public void write(int x) {
        super.write(x);
        _dupOut.write(x);
        return;
    }
    public void write(byte[] x, int o, int l) {
        super.write(x, o, l);
        _dupOut.write(x, o, l);
        return;
    }
}

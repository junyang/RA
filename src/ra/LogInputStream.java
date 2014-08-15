package ra;

import java.io.*;

public class LogInputStream extends InputStream {
    protected InputStream _in;
    protected OutputStream _out;
    public LogInputStream(InputStream in, OutputStream out) {
        _out = out;
        _in = in;
        return;
    }
    public int available() throws IOException {
        return _in.available();
    }
    public void close() throws IOException {
        _in.close();
    }
    public void mark(int readlimit) {
        _in.mark(readlimit);
    }
    public boolean markSupported() {
        return _in.markSupported();
    }
    public int read() throws IOException {
        int b = _in.read();
        _out.write(b);
        _out.flush();
        return b;
    }
    public int read(byte[] b) throws IOException {
        int numbytes = _in.read(b);
        if (numbytes >= 0) {
            _out.write(b, 0, numbytes);
            _out.flush();
        }
        return numbytes;
    }
    public int read(byte[] b, int off, int len) throws IOException {
        int numbytes = _in.read(b, off, len);
        if (numbytes >= 0) {
            _out.write(b, 0, numbytes);
            _out.flush();
        }
        return numbytes;
    }
    public void reset() throws IOException {
        _in.reset();
    }
    public long skip(long n) throws IOException {
        return _in.skip(n);
    }
}

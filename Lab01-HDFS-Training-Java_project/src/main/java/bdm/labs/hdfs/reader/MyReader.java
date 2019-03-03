package bdm.labs.hdfs.reader;

import java.io.IOException;

public interface MyReader {
	
	public void open(String file) throws IOException;
	
	public String next() throws IOException;
	
	public void close() throws IOException;

}

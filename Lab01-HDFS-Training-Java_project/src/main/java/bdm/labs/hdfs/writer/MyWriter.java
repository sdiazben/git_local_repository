package bdm.labs.hdfs.writer;

import java.io.IOException;

import adult.avro.Adult;

public interface MyWriter {
	
	public void open(String file) throws IOException;
	
	public void put(Adult w);
	
	public void reset();
	
	public int flush() throws IOException;
	
	public void close() throws IOException;
	
}

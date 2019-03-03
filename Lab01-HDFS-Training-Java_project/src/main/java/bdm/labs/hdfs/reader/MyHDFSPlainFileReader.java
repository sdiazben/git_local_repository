package bdm.labs.hdfs.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MyHDFSPlainFileReader implements MyReader {
	
	private Configuration config;
	private FileSystem fs;
	private BufferedReader input;

	public MyHDFSPlainFileReader() {
		try {
			this.config = new Configuration();
			this.fs = FileSystem.get(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void open(String file) throws IOException {
		Path path = new Path(file);
		if (!this.fs.exists(path)) {
			System.out.println("File "+file+" does not exist!");
			System.exit(1);
		}
		this.input = new BufferedReader(new InputStreamReader(this.fs.open(path)));
	}
	
	public String next() throws IOException {
		String line = null;
		try {
			line = this.input.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return line;
	}
	
	public void close() throws IOException {
		try {
			this.input.close();
			this.fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

package bdm.labs.hdfs.reader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class MyHDFSSequenceFileReader implements MyReader {

	private Configuration config;
	private FileSystem fs;
	private SequenceFile.Reader reader;
	
	public MyHDFSSequenceFileReader() {
		try {
			this.config = new Configuration();
            config.addResource(new Path("/home/bdm/BDM-Software/hadoop-2.8.0/etc/hadoop/core-site.xml"));
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
		SequenceFile.Reader.Option[] options = new SequenceFile.Reader.Option[]
		{
				SequenceFile.Reader.file(path)
		};
		this.reader = new SequenceFile.Reader(this.config, options);
	}
	
	public String next() throws IOException {
		Text key = new Text();
		Text value = new Text();
		if (this.reader.next(key, value)) {
			return key.toString()+'\t'+value.toString();
		}
		return null;
	}
	
	public void close() throws IOException {
		this.reader.close();
		this.fs.close();
	}
	
}

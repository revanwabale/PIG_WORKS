//javac -classpath `hadoop classpath`:`hbase classpath`:../LIB/pig-0.16.0.2.5.5.0-157-core-h2.jar MoveFiles.java

package blms.batches.spcl;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class MoveFiles extends EvalFunc<String> {
	//private static String namenode_qualifier_pattern="hdfs:\\/";
	//private static String namenode_qualifier = "hdfs://hahdfsqa/";
	public String exec(Tuple input) throws IOException  {

		if (input == null || input.size() == 0) {
			System.out.println("Need to specify arguments <src> <dst>");
			return null;
		}

		Configuration conf = new Configuration(); 
		Path srcPath;
		conf.set("fs.defaultFS", "hdfs://hahdfsqa"); 
		//conf.addResource("/etc/hadoop/conf/core-site.xml"); --> not worked.
		try {
		srcPath = new Path(String.valueOf(input.get(0)));
		}catch(ExecException e) {
			return String.valueOf(input.get(0)) + "\tMOVING_FAILED:Exception while converting into SrcString to SrcPath"+e.getMessage();
		}
		try {
		FileSystem srcFs = srcPath.getFileSystem(conf); 
		if(!srcFs.exists(srcPath)) 
			return String.valueOf(input.get(0)) + "\tMOVING_FAILED:FILE_DOES_NOT_EXIST";

		
		Path dstPath = new Path(String.valueOf(input.get(1))); 
		FileSystem dstFs = dstPath.getFileSystem(conf); 

		if(!dstFs.exists(dstPath)) 
			dstFs.mkdirs(dstPath);
		
		//String str = srcPath.toString();

		//String QulifiedString = replaceWithPattern(str,namenode_qualifier);
		//Path nwSrcPath = new Path(QulifiedString);
		
		Boolean status = FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf); 
		//Boolean status = FileUtil.copy(srcFs, nwSrcPath, dstFs, dstPath, false, conf); 

		if(status) 
			return String.valueOf(input.get(0)) + "\t" + String.valueOf(input.get(2));

		return String.valueOf(input.get(0)) + "\tMOVING_FAILED:DETAILS_NOT_AVAILABLE";
		}catch(ExecException e) {
			return String.valueOf(input.get(0)) + "\tMOVING_FAILED:Exception while getting SRC,DEST filesystem from SrcPath & DESTPath"+e.getMessage();
		}
	}
	
	
	/* public String replaceWithPattern(String str,String replace){
	         
	        Pattern ptn = Pattern.compile(namenode_qualifier_pattern);
	        Matcher mtch = ptn.matcher(str);
	        return mtch.replaceAll(replace);
	    }*/
}

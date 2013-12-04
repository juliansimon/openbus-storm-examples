/*
* Copyright 2013 Produban
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.produban.openbus.processor.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.produban.openbus.processor.properties.Conf;

/**
 * Store HDFS    
 */
public class HDFSStore {
	private final static Logger LOG = LoggerFactory.getLogger(HDFSStore.class);
	
    FileSystem fileSystem;	
    Configuration configuration;

    public HDFSStore() {   
    	configuration = new Configuration();     	
	    configuration.set("fs.defaultFS", Conf.HDFS_DIR);                    
	    configuration.set("hadoop.job.ugi", Conf.HDFS_USER);	    	    
	    System.setProperty("HADOOP_USER_NAME", Conf.HDFS_USER);
	      
	    fileSystem = HDFSUtils.getFS(Conf.HDFS_DIR, configuration);
    }

    public HDFSStore(String hdfsDir, String hdfsUser) {   
    	configuration = new Configuration();     	
	    configuration.set("fs.defaultFS", hdfsDir);                    	    
	    System.setProperty("HADOOP_USER_NAME", hdfsUser);
	      
	    fileSystem = HDFSUtils.getFS(Conf.HDFS_DIR, configuration);
    }
        
	public void copyFromLocalFile(String pathLocal, String pathHDFS) throws IOException {
		try {						
			fileSystem.copyFromLocalFile(new Path(pathLocal), new Path(Conf.HDFS_DIR + File.pathSeparator + pathHDFS));						
		} catch (IOException e) {
			LOG.error("Error writing to hdfs: " + pathHDFS + " " + e);
			throw new RuntimeException(e);
		}
	}     

    public void writeFile2HDFS(String path, String toWrite) throws IOException {
        String tmp = Conf.HDFS_DIR + File.separator + path + ".tmp";
        FSDataOutputStream os = fileSystem.create(new Path(tmp), true);
        os.writeUTF(toWrite);
        os.close();
        fileSystem.rename(new Path(tmp), new Path(Conf.HDFS_DIR + File.separator + path));
    }
}
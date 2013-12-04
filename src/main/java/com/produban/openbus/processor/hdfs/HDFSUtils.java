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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.log4j.Logger;

/**
 * HDFS utils     
 */
public class HDFSUtils {
    public static final Logger LOG = Logger.getLogger(HDFSUtils.class);
    public static final String URI_CONFIG = "file://localhost/";
    
    public static FileSystem getFS(String path, Configuration conf) {
        try {        	
            FileSystem ret = new Path(path).getFileSystem(conf);

            if(ret instanceof LocalFileSystem) {
                LOG.info("Using local filesystem and disabling checksums");
                ret = new RawLocalFileSystem();
                
                try {
                    ((RawLocalFileSystem) ret).initialize(new URI(URI_CONFIG), new Configuration());
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
            	LOG.info("No local filesystem " + conf.getStrings("fs.defaultFS"));
            }
            
            return ret;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }   
}

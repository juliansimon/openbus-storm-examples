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

package com.produban.openbus.processor;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.produban.openbus.processor.util.FormatUtil;

/**
 * Unit test for UtilTest.
 */
public class UtilTest extends TestCase {
    
	private static Logger LOG = LoggerFactory.getLogger(UtilTest.class);
	
    public UtilTest(String testName) {
        super( testName );
    }

    public static Test suite() {
        return new TestSuite(UtilTest.class);
    }

    public void testFormatDate() {		    		
		String oldScheduledDate = "[17/Sep/2012:19:01:24+0200]";
				
		String dateFormat = FormatUtil.getDateInFormatMinute(oldScheduledDate);
		assertNotNull(dateFormat);
		
		dateFormat = FormatUtil.getDateInFormatHour(oldScheduledDate);
		assertNotNull(dateFormat);

		dateFormat = FormatUtil.getDateInFormatDay(oldScheduledDate);
		assertNotNull(dateFormat);
		
	    LOG.info("Date: " + dateFormat + "\n");	
    }
    
    public void testFormatAvroDate() {		    	
		String oldScheduledDate = "Tue Nov 05 07:34:39 EST 2013";
		long dateFormat = 0;
		
		try {		
			dateFormat = FormatUtil.getDateInFormat10Second(oldScheduledDate);
		}
		catch (Exception e) {
			assertTrue(false);
		}
		
		assertTrue(true);		
	    LOG.info("Date: " + String.valueOf(dateFormat));	
    }
}

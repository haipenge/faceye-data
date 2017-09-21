package com.faceye.test.component.data.conf;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.faceye.component.data.conf.Configuration;



@RunWith(JUnit4.class)
public class ConfigurationTestCase {
	
	@Test
	public void testGet() throws Exception{
		String testDir=Configuration.get("test.dir");
		Assert.assertTrue(StringUtils.equals(testDir, "/home"));
	}
}

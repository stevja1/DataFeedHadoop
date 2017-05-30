package com.datafeedtoolbox.examples;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class SkipPatternsTest {
	@Ignore
	@Test
	public void test() {
		String[] patterns = {
						"\\(", "\\)", "\\[", "\\]", "\\\\", ";", "'", ",", ".", "/",
						"\\{", "\\}", "\\|", ":", "\"", "\\<", "\\>", "\\?", "`", "!",
						"@", "#", "$", "%", "^", "&", "\\*", "-", "=", "_",
						"\\+"
		};
		String testString = "";
		int i = 0;
		for(String pattern : patterns) {
			System.out.println(String.format("Processing pattern: %d [%s]", i, pattern));
			testString = testString.replaceAll(pattern, "");
			++i;
		}
	}

	@Test
	public void splitTest() {
		final String testData = "field1\tfield2\tfield3";
		String[] parts = testData.split("\\t");
		System.out.println("Parts.length: "+parts.length);
	}
}

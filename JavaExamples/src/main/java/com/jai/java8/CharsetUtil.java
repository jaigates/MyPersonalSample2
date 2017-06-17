package com.jai.java8;

import java.io.IOException;

import org.mozilla.universalchardet.UniversalDetector;

public class CharsetUtil {
	
	public static void main(String[] args) throws IOException {
		
		//getCharset("10rows.csv");
		getCharset("majestic_million.csv");
	}

	public static String getCharset(String fileName) throws IOException {

		byte[] buf = new byte[4096];
		
		java.io.FileInputStream fis = new java.io.FileInputStream(fileName);

		UniversalDetector detector = new UniversalDetector(null);

		// (2)
		int nread;
		while ((nread = fis.read(buf)) > 0 && !detector.isDone()) {
			detector.handleData(buf, 0, nread);
		}
		// (3)
		detector.dataEnd();

		// (4)
		String encoding = detector.getDetectedCharset();
		if (encoding != null) {
			System.out.println("Detected encoding = " + encoding);
		} else {
			System.out.println("No encoding detected.");
		}

		// (5)
		detector.reset();
		return encoding;

	}
}

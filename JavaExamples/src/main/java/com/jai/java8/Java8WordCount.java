package com.jai.java8;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

public class Java8WordCount {
	
	public static Logger log = LoggerFactory.getLogger(Java8WordCount.class);

	public static void main(String[] args) {
		
		//String fn =("./big.txt");
		//String fn = "./500krows.csv";
		String fn = "majestic_million.csv";
		StopWatch sw = new StopWatch(fn);
		sw.start("wordcount");
		Path path = Paths.get(fn);
		String DELIMITER = ",";
		log.info(path.toAbsolutePath()+"-"+path.toFile().exists());
		
		try {
			Stream<String> lines = Files.lines(path,Charset.forName(CharsetUtil.getCharset(fn)));
			//count(lines);
			Stream<String> flatMap = lines.flatMap(line ->
				{
					String[] split = line.trim().split(DELIMITER);
					return Arrays.stream(split);
				});
			//count(flatMap);
			//count(filter);
			Map<String, Integer> wordCount = flatMap.map(word -> word.toLowerCase().trim()).filter(word -> word.length() > 0)
					.map(word -> new SimpleEntry<>(word, 1)).sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
					.reduce(new LinkedHashMap<>(), (acc, entry) -> {
						acc.put(entry.getKey(), acc.compute(entry.getKey(), (k, v) -> v == null ? 1 : v + 1));
						return acc;
					}, (m1, m2) -> m1);
			lines.close();
			sw.stop();
			log.info("Total time taken : {}", sw.shortSummary());
			sw.start("forEach");
			wordCount.forEach((k, v) -> log.info(String.format("%s ----- %d", k, v)));
		} catch (Exception e) {
			e.printStackTrace();
			log.error("",e);
		}
		
		sw.stop();
		log.info(sw.prettyPrint());
		
		
		int fordebug = 0;

	}

	private static void count(Stream<?> filter) {
		
		long count = filter.count();
		log.info("Stream count : "+count);
		
	}
}

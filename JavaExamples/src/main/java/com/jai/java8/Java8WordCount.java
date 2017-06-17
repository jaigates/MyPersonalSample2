package com.jai.java8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Java8WordCount {
	
	public static Logger log = LoggerFactory.getLogger(Java8WordCount.class);

	public static void main(String[] args) throws IOException {

		Path path = Paths.get("./pom.xml");
		System.out.println(path.getFileName());
		Stream<String> lines = Files.lines(path);
		Stream<String> flatMap = lines.flatMap(line -> Arrays.stream(line.trim().split(" ")));
		Stream<String> filter = flatMap.map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim()).filter(word -> word.length() > 0);
		
		count(filter);
		//Map<String, Integer> wordCount = 
				/*
				.map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim()).filter(word -> word.length() > 0)
				.map(word -> new SimpleEntry<>(word, 1)).sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
				.reduce(new LinkedHashMap<>(), (acc, entry) -> {
					acc.put(entry.getKey(), acc.compute(entry.getKey(), (k, v) -> v == null ? 1 : v + 1));
					return acc;
				}, (m1, m2) -> m1);

		wordCount.forEach((k, v) -> System.out.println(String.format("%s ==>> %d", k, v)));
		*/
		lines.close();

		int fordebug = 0;

	}

	private static void count(Stream<?> filter) {
		
		long count = filter.count();
		log.info("Stream count : "+count);
		
	}
}

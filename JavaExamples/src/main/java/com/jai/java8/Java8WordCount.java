package com.jai.java8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class Java8WordCount {

	public static void main(String[] args) throws IOException {

		Path path = Paths.get("./pom.xml");
		System.out.println(path.getFileName());
		Stream<String> lines = Files.lines(path);
		Map<String, Integer> wordCount = lines.flatMap(line -> Arrays.stream(line.trim().split(" ")))
				.map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim()).filter(word -> word.length() > 0)
				.map(word -> new SimpleEntry<>(word, 1)).sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
				.reduce(new LinkedHashMap<>(), (acc, entry) -> {
					acc.put(entry.getKey(), acc.compute(entry.getKey(), (k, v) -> v == null ? 1 : v + 1));
					return acc;
				}, (m1, m2) -> m1);

		wordCount.forEach((k, v) -> System.out.println(String.format("%s ==>> %d", k, v)));
		long count = lines.count();

		int fordebug = 0;

	}
}

package com.jai.java8;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

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

	public static final String DELIMITER = ",";

	public static void main(String[] args) throws IOException {

		// String fn =("./big.txt");
		// String fn = "./500krows.csv";
		String fn = "majestic_million.csv";
		// wordcount1(fn,"wordcount1");
		wordcount2(fn, "wordcount2");

	}

	public static void wordcount1(String fn, String logmsg) {

		StopWatch sw = new StopWatch(fn);
		sw.start(logmsg);
		Path path = Paths.get(fn);

		log.info(path.toAbsolutePath() + "-" + path.toFile().exists());

		try {
			Stream<String> lines = Files.lines(path, Charset.forName(CharsetUtil.getCharset(fn)));
			// count(lines);
			Stream<String> flatMap = lines.flatMap(line -> {
				String[] split = line.trim().split(DELIMITER);
				return Arrays.stream(split);
			});
			// count(flatMap);
			// count(filter);
			Map<String, Long> wordCount = flatMap.map(word -> word.trim()).filter(word -> word.length() > 0).map(word -> new SimpleEntry<>(word, 1))
					.sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey())).reduce(new LinkedHashMap<>(), (acc, entry) -> {
						acc.put(entry.getKey(), acc.compute(entry.getKey(), (k, v) -> v == null ? 1 : v + 1));
						return acc;
					}, (m1, m2) -> m1);
			lines.close();
			// print(wordCount);

		} catch (Exception e) {
			e.printStackTrace();
			log.error("", e);
		}

		sw.stop();
		log.info(sw.prettyPrint());

		int fordebug = 0;
	}

	private static void print(Map<String, Long> wordCount) {
		StopWatch sw = new StopWatch("print");
		sw.start("print");
		wordCount.forEach((k, v) -> log.info(String.format("%s ----- %d ", k, v)));
		sw.stop();
		log.info(sw.prettyPrint());
	}

	public static Map<String, Long> wordcount2(String fn, String logmsg) throws IOException {
		StopWatch sw = new StopWatch(fn);
		sw.start(logmsg);
		Stream<String> lines = Files.lines(Paths.get(fn), Charset.forName(CharsetUtil.getCharset(fn)));
		Stream<String> flatMap = lines.flatMap(line -> {
			String[] split = line.trim().split(DELIMITER);
			return Arrays.stream(split);
		});
		Map<String, Long> wordCount = flatMap.collect(groupingBy(name -> name, counting()));
		sw.stop();
		print(wordCount);
		log.info(sw.prettyPrint());
		return wordCount;
	}

	private static void count(Stream<?> filter) {

		long count = filter.count();
		log.info("Stream count : " + count);

	}
}

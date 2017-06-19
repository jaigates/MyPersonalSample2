package com.jai.java8;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
	
	public static Path mOutputPath = Paths.get("./output/wcjava.txt");

	public static void main(String[] args) throws IOException {

		// String fn =("./data/big.txt");
		// String fn = "./data/500krows.csv";
		String fn = "./data/majestic_million.csv";
		wordcount1(fn,"wordcount1::"+fn);
		wordcount2(fn, "wordcount2::"+fn);

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
			writeMap(wordCount);
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
		//Files.write("./output/java8wc2", bytes, options);
		sw.stop();
		sw.start("flatMap.collect(groupingBy(name -> name, counting()))");
		Map<String, Long> wordCount = flatMap.collect(groupingBy(name -> name, counting()));
		
	/*	Files.write(Paths.get("output/1"), () -> mHashMap.entrySet().stream()
			    .<CharSequence>map(e -> e.getKey() + DATA_SEPARATOR + e.getValue())
			    .iterator());*/
		sw.stop();
		writeMap(wordCount);
		//print(wordCount);
		log.info(sw.prettyPrint());
		return wordCount;
	}

	private static void writeMap(Map<String, Long> wordCount) throws IOException {
		StopWatch sw = new StopWatch("writeMap");
		sw.start("Files.newBufferedWriter");
		Writer writer = Files.newBufferedWriter(mOutputPath);
		wordCount.forEach((key, value) -> {
			try {
				writer.write(key + "--" + value + System.lineSeparator());
			} catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
		});
		sw.stop();
		log.info(sw.shortSummary());

	}

	private static void count(Stream<?> filter) {

		long count = filter.count();
		log.info("Stream count : " + count);

	}
	
	public static void write(byte[] buffer ) throws IOException {
		int number_of_lines = 400000;
		FileChannel rwChannel = new RandomAccessFile("textfile.txt", "rw").getChannel();
		ByteBuffer wrBuf = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, buffer.length * number_of_lines);
		for (int i = 0; i < number_of_lines; i++)
		{
		    wrBuf.put(buffer);
		}
		rwChannel.close();
		
		
		
	}
}

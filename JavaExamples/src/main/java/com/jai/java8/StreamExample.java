package com.jai.java8;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamExample {

	public static void main(String[] args) {
		List<String> collected = Stream.of("a", "b", "hello") // Stream of
																// String
				.map(String::toUpperCase) // Returns a stream consisting of the
											// results of applying the given
											// function to the elements of this
											// stream.
				.collect(Collectors.toList());
		System.out.println(collected);
	}

}

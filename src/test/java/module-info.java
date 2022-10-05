module tdlib.reactive.api.test {
	exports it.tdlight.reactiveapi.test;
	requires org.apache.logging.log4j.core;
	requires org.slf4j;
	requires tdlib.reactive.api;
	requires org.junit.jupiter.api;
	requires reactor.core;
	requires com.google.common;
	requires it.unimi.dsi.fastutil;
	requires org.reactivestreams;
	requires kafka.clients;
}
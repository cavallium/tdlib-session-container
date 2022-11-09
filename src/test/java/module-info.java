module tdlib.reactive.api.test {
	exports it.tdlight.reactiveapi.test;
	requires org.apache.logging.log4j.core;
	requires tdlib.reactive.api;
	requires org.junit.jupiter.api;
	requires reactor.core;
	requires com.google.common;
	requires it.unimi.dsi.fastutil;
	requires org.reactivestreams;
	requires kafka.clients;
	requires java.logging;
	requires rsocket.core;
	requires org.junit.platform.engine;
	requires org.junit.platform.commons;
	requires org.junit.jupiter.engine;
	requires reactor.tools;
	requires rsocket.transport.netty;
	requires reactor.test;
	requires reactor.netty.core;
	requires org.apache.logging.log4j;
	requires filequeue;
	requires rsocket.transport.local;
}
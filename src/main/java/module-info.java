module tdlib.reactive.api {
	exports it.tdlight.reactiveapi;
	exports it.tdlight.reactiveapi.generated;
	requires com.fasterxml.jackson.annotation;
	requires org.jetbrains.annotations;
	requires org.slf4j;
	requires tdlight.java;
	requires org.reactivestreams;
	requires tdlight.api;
	requires com.google.common;
	requires java.logging;
	requires kafka.clients;
	requires org.apache.logging.log4j;
	requires reactor.kafka;
	requires com.fasterxml.jackson.databind;
	requires com.fasterxml.jackson.dataformat.yaml;
	requires static io.soabase.recordbuilder.core;
	requires java.compiler;
	requires it.unimi.dsi.fastutil;
	requires net.minecrell.terminalconsole;
	requires org.jline.reader;
	requires jdk.unsupported;
	requires jakarta.xml.bind;
	requires reactor.core;
}
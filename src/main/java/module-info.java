module it.tdlight.tdlibsessioncontainer {
	requires vertx.core;
	requires org.jetbrains.annotations;
	requires reactor.core;
	requires tdlight.java;
	requires org.slf4j;
	requires org.reactivestreams;
	requires org.apache.logging.log4j.core;
	requires org.apache.logging.log4j;
	requires common.utils;
	requires vertx.circuit.breaker;
	requires org.apache.commons.lang3;
	requires com.hazelcast.core;
	requires vertx.hazelcast;
	requires it.unimi.dsi.fastutil;
	requires com.google.gson;

	exports it.tdlight.tdlibsession;
	exports it.tdlight.tdlibsession.remoteclient;
	exports it.tdlight.tdlibsession.td;
	exports it.tdlight.tdlibsession.td.direct;
	exports it.tdlight.tdlibsession.td.easy;
	exports it.tdlight.tdlibsession.td.middle;
	exports it.tdlight.tdlibsession.td.middle.client;
	exports it.tdlight.tdlibsession.td.middle.direct;
	exports it.tdlight.tdlibsession.td.middle.server;
	exports it.tdlight.utils;
}
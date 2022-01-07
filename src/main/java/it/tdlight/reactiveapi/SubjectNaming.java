package it.tdlight.reactiveapi;

public class SubjectNaming {

	public static String getDynamicIdResolveSubject(long userId) {
		return "session-" + userId + "-dynamic-live-id-resolve";
	}
}

package it.tdlight.tdlibsession;

public class VariableWrapper<T> {

	public volatile T var;

	public VariableWrapper(T value) {
		this.var = value;
	}
}

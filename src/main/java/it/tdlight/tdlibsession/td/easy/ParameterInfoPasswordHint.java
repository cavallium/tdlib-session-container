package it.tdlight.tdlibsession.td.easy;

public class ParameterInfoPasswordHint implements ParameterInfo {
	private final String hint;

	public ParameterInfoPasswordHint(String hint) {
		this.hint = hint;
	}

	public String getHint() {
		return hint;
	}
}

package org.himanshu.helper;

public class PropertiesHelper {

	private String[] args;
	private Integer argCount;

	public PropertiesHelper(String[] args, Integer argCount) {
		this.args = args;
		this.argCount = argCount;
	}

	private void validateArgs() {

		if (args.length == 0) {
			throw new IllegalArgumentException("Length of argument is incorrect. Expected Size " + argCount);
		} else if (args.length != argCount) {
			throw new IllegalArgumentException("Length of argument is incorrect. Expected Size " + argCount);
		}

	}

	public String getValue(String key) {
		validateArgs();
		String value = null;
		boolean found = false;

		for (String arg : args) {
			String[] arr = arg.split("=");

			if (arr[0].equals(key)) {
				value = arr[1];
				found = true;
			}
		}

		if (!found) {
			throw new IllegalArgumentException("Correct values not passed");
		}

		return value;
	}

}

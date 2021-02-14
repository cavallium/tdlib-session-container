package it.tdlight.tdlibsession.remoteclient;

import io.vertx.core.file.FileSystemException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.StringJoiner;

public class SecurityInfo {

	private final Path keyStorePath;
	private final Path keyStorePasswordPath;
	private final Path trustStorePath;
	private final Path trustStorePasswordPath;

	public SecurityInfo(Path keyStorePath, Path keyStorePasswordPath, Path trustStorePath, Path trustStorePasswordPath) {
		this.keyStorePath = keyStorePath;
		this.keyStorePasswordPath = keyStorePasswordPath;
		this.trustStorePath = trustStorePath;
		this.trustStorePasswordPath = trustStorePasswordPath;
	}

	public Path getKeyStorePath() {
		return keyStorePath;
	}

	public Path getKeyStorePasswordPath() {
		return keyStorePasswordPath;
	}

	public String getKeyStorePassword(boolean required) {
		try {
			if (Files.isReadable(keyStorePasswordPath) && Files.size(keyStorePasswordPath) >= 6) {
				return Files.readString(keyStorePasswordPath, StandardCharsets.UTF_8).split("\n")[0];
			} else if (required) {
				throw new NoSuchElementException("No keystore password is set on '" + keyStorePasswordPath.toString() + "'");
			}
		} catch (IOException ex) {
			throw new FileSystemException(ex);
		}
		return null;
	}

	public Path getTrustStorePath() {
		return trustStorePath;
	}

	public Path getTrustStorePasswordPath() {
		return trustStorePasswordPath;
	}

	public String getTrustStorePassword(boolean required) {
		try {
			if (Files.isReadable(trustStorePasswordPath) && Files.size(trustStorePasswordPath) >= 6) {
				return Files.readString(trustStorePasswordPath, StandardCharsets.UTF_8).split("\n")[0];
			} else if (required) {
				throw new NoSuchElementException("No truststore password is set on '" + trustStorePasswordPath.toString() + "'");
			}
		} catch (IOException ex) {
			throw new FileSystemException(ex);
		}
		return null;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", SecurityInfo.class.getSimpleName() + "[", "]")
				.add("keyStorePath=" + keyStorePath)
				.add("keyStorePasswordPath=" + keyStorePasswordPath)
				.add("trustStorePath=" + trustStorePath)
				.add("trustStorePasswordPath=" + trustStorePasswordPath)
				.toString();
	}
}

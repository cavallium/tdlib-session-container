package it.tdlight.tdlibsession.td.easy;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

public class TdEasySettings {
	public final boolean useTestDc;
	public final String databaseDirectory;
	public final String filesDirectory;
	public final boolean useFileDatabase;
	public final boolean useChatInfoDatabase;
	public final boolean useMessageDatabase;
	public final int apiId;
	public final String apiHash;
	public final String systemLanguageCode;
	public final String deviceModel;
	public final String systemVersion;
	public final String applicationVersion;
	public final boolean enableStorageOptimizer;
	public final boolean ignoreFileNames;
	private final Long phoneNumber;
	private final String botToken;

	public TdEasySettings(boolean useTestDc,
			String databaseDirectory,
			String filesDirectory,
			boolean useFileDatabase,
			boolean useChatInfoDatabase,
			boolean useMessageDatabase,
			int apiId,
			String apiHash,
			String systemLanguageCode,
			String deviceModel,
			String systemVersion,
			String applicationVersion,
			boolean enableStorageOptimizer,
			boolean ignoreFileNames,
			@Nullable Long phoneNumber,
			@Nullable String botToken) {
		this.useTestDc = useTestDc;
		this.databaseDirectory = databaseDirectory;
		this.filesDirectory = filesDirectory;
		this.useFileDatabase = useFileDatabase;
		this.useChatInfoDatabase = useChatInfoDatabase;
		this.useMessageDatabase = useMessageDatabase;
		this.apiId = apiId;
		this.apiHash = apiHash;
		this.systemLanguageCode = systemLanguageCode;
		this.deviceModel = deviceModel;
		this.systemVersion = systemVersion;
		this.applicationVersion = applicationVersion;
		this.enableStorageOptimizer = enableStorageOptimizer;
		this.ignoreFileNames = ignoreFileNames;
		this.phoneNumber = phoneNumber;
		this.botToken = botToken;
		if ((phoneNumber == null) == (botToken == null)) {
			throw new IllegalArgumentException("You must set a phone number or a bot token");
		}
	}

	public boolean isPhoneNumberSet() {
		return phoneNumber != null;
	}

	public long getPhoneNumber() {
		return Objects.requireNonNull(phoneNumber, "You must set a phone number");
	}

	public boolean isBotTokenSet() {
		return botToken != null;
	}

	public String getBotToken() {
		return Objects.requireNonNull(botToken, "You must set a bot token");
	}

	public static Builder newBuilder() {
		return new Builder();
	}


	public static class Builder {
		private boolean useTestDc = false;
		private String databaseDirectory = "jtdlib-database";
		private String filesDirectory = "jtdlib-files";
		private boolean useFileDatabase = true;
		private boolean useChatInfoDatabase = true;
		private boolean useMessageDatabase = true;
		private int apiId = 376588;
		private String apiHash = "2143fdfc2bbba3ec723228d2f81336c9";
		private String systemLanguageCode = "en";
		private String deviceModel = "JTDLib";
		private String systemVersion = "JTDLib";
		private String applicationVersion = "1.0";
		private boolean enableStorageOptimizer = false;
		private boolean ignoreFileNames = false;
		@Nullable
		private Long phoneNumber = null;
		@Nullable
		private String botToken = null;

		private Builder() {

		}

		public boolean isUseTestDc() {
			return useTestDc;
		}

		public Builder setUseTestDc(boolean useTestDc) {
			this.useTestDc = useTestDc;
			return this;
		}

		public String getDatabaseDirectory() {
			return databaseDirectory;
		}

		public Builder setDatabaseDirectory(String databaseDirectory) {
			this.databaseDirectory = databaseDirectory;
			return this;
		}

		public String getFilesDirectory() {
			return filesDirectory;
		}

		public Builder setFilesDirectory(String filesDirectory) {
			this.filesDirectory = filesDirectory;
			return this;
		}

		public boolean isUseFileDatabase() {
			return useFileDatabase;
		}

		public Builder setUseFileDatabase(boolean useFileDatabase) {
			this.useFileDatabase = useFileDatabase;
			return this;
		}

		public boolean isUseChatInfoDatabase() {
			return useChatInfoDatabase;
		}

		public Builder setUseChatInfoDatabase(boolean useChatInfoDatabase) {
			this.useChatInfoDatabase = useChatInfoDatabase;
			return this;
		}

		public boolean isUseMessageDatabase() {
			return useMessageDatabase;
		}

		public Builder setUseMessageDatabase(boolean useMessageDatabase) {
			this.useMessageDatabase = useMessageDatabase;
			return this;
		}

		public int getApiId() {
			return apiId;
		}

		public Builder setApiId(int apiId) {
			this.apiId = apiId;
			return this;
		}

		public String getApiHash() {
			return apiHash;
		}

		public Builder setApiHash(String apiHash) {
			this.apiHash = apiHash;
			return this;
		}

		public String getSystemLanguageCode() {
			return systemLanguageCode;
		}

		public Builder setSystemLanguageCode(String systemLanguageCode) {
			this.systemLanguageCode = systemLanguageCode;
			return this;
		}

		public String getDeviceModel() {
			return deviceModel;
		}

		public Builder setDeviceModel(String deviceModel) {
			this.deviceModel = deviceModel;
			return this;
		}

		public String getSystemVersion() {
			return systemVersion;
		}

		public Builder setSystemVersion(String systemVersion) {
			this.systemVersion = systemVersion;
			return this;
		}

		public String getApplicationVersion() {
			return applicationVersion;
		}

		public Builder setApplicationVersion(String applicationVersion) {
			this.applicationVersion = applicationVersion;
			return this;
		}

		public boolean isEnableStorageOptimizer() {
			return enableStorageOptimizer;
		}

		public Builder setEnableStorageOptimizer(boolean enableStorageOptimizer) {
			this.enableStorageOptimizer = enableStorageOptimizer;
			return this;
		}

		public boolean isIgnoreFileNames() {
			return ignoreFileNames;
		}

		public Builder setIgnoreFileNames(boolean ignoreFileNames) {
			this.ignoreFileNames = ignoreFileNames;
			return this;
		}

		public Builder setPhoneNumber(long phoneNumber) {
			this.phoneNumber = phoneNumber;
			return this;
		}

		public Builder setBotToken(String botToken) {
			this.botToken = botToken;
			return this;
		}

		public TdEasySettings build() {
			return new TdEasySettings(useTestDc,
					databaseDirectory,
					filesDirectory,
					useFileDatabase,
					useChatInfoDatabase,
					useMessageDatabase,
					apiId,
					apiHash,
					systemLanguageCode,
					deviceModel,
					systemVersion,
					applicationVersion,
					enableStorageOptimizer,
					ignoreFileNames,
					phoneNumber,
					botToken
			);
		}
	}
}

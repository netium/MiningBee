package org.ctp.server.configuration;

public class ServerConfigurationLoadException extends Exception {
    private String configurationFilename;

    public ServerConfigurationLoadException(final String configurationFilename) {
        this.configurationFilename = configurationFilename;
    }

    public ServerConfigurationLoadException(final String configurationFilename, Throwable cause) {
        super(cause);
        this.configurationFilename = configurationFilename;
    }

    public String getConfigurationFilename() {
        return configurationFilename;
    }

    @Override
    public String toString() {
        return ServerConfigurationLoadException.class.getName() + ": " + "The server configuration file: " + configurationFilename + " cannot be loaded";
    }
}

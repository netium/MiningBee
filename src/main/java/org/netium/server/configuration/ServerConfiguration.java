/*
 * Copyright (c) 2018.
 *
 * Author: Netium (Bo Zhou)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.netium.server.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public final class ServerConfiguration {

    private String mode;
    private String dbPath;
    private Map<String, String> cluster;
    private Map<String, String> single;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getDbPath() {
        return dbPath;
    }

    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }

    public Map<String, String> getCluster() {
        return cluster;
    }

    public void setCluster(Map<String, String> cluster) {
        this.cluster = cluster;
    }

    public Map<String, String> getSingle() {
        return single;
    }

    public void setSingle(Map<String, String> single) {
        this.single = single;
    }

    public static ServerConfiguration loadFromFile(String filename) throws ServerConfigurationLoadException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        ServerConfiguration config = null;
        try {
            config = mapper.readValue(new File(filename), ServerConfiguration.class);
        } catch (IOException e) {
            throw new ServerConfigurationLoadException(filename, e);
        }
        System.out.println("Server Configuration:" + ReflectionToStringBuilder.toString(config, ToStringStyle.MULTI_LINE_STYLE));


        return config;
    }
}

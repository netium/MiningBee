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

package org.netium.server.storageengine;

import org.netium.server.storageengine.command.ResultHandler;

import java.io.Closeable;

public interface StorageEngine extends Closeable {
    void start();
    void put(String key, String value, ResultHandler resultHandler);
    void read(String key, ResultHandler resultHandler);
    void delete(String key, ResultHandler resultHandler);
    void compareAndSet(String key, String oldValue, String newValue, ResultHandler resultHandler);
    void getDiagnosisInfo(ResultHandler resultHandler);
    void flush(ResultHandler resultHandler);
}

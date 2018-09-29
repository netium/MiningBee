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

package org.ctp.server.storageengine.lsm.sstable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class SSTableConstants {
    public static final Charset SECTION_HEADER_CHARSET = StandardCharsets.US_ASCII;

    public static final String DATA_SECTION_NAME_STRING = "DAT";
    public static final byte[] DATA_SECTION_NAME_BYTES = DATA_SECTION_NAME_STRING.getBytes(SECTION_HEADER_CHARSET);

    public static final String BLOOMFILTER_SECTION_NAME_STRING = "BLM";
    public static final byte[] BLOOMFILTER_SECTION_NAME_BYTES = BLOOMFILTER_SECTION_NAME_STRING.getBytes(SECTION_HEADER_CHARSET);

    public static final String INDEX_SECTION_NAME_STRING = "IDX";
    public static final byte[] INDEX_SECTION_NAME_BYTES = INDEX_SECTION_NAME_STRING.getBytes(SECTION_HEADER_CHARSET);

    public static final String SUMMARY_SECTION_NAME_STRING = "SUM";
    public static final byte[] SUMMARY_SECTION_NAME_BYTES = SUMMARY_SECTION_NAME_STRING.getBytes(SECTION_HEADER_CHARSET);

    private SSTableConstants() {}
}

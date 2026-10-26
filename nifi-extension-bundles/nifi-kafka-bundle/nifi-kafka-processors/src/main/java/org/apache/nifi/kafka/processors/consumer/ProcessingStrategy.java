/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.kafka.processors.consumer;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of supporting strategies for consuming and serializing Kafka Records
 */
public enum ProcessingStrategy implements DescribedValue {
    FLOW_FILE("Write one FlowFile for each Kafka Record consumed. "
            + "A null value is an empty FlowFile with kafka.tombstone set to true; a non-null empty value does not set that attribute."),

    DEMARCATOR("Write one FlowFile for each batch of Kafka Records consumed (optionally grouped by Kafka key). "
            + "A null value is skipped and its offset is still committed. A non-null empty value is included in the bundle. "
            + "A trailing null value is still committed, and that commit offset can be higher than the FlowFile attribute kafka.max.offset."),

    RECORD("Write one FlowFile containing multiple Kafka Records consumed and processed with Record Reader and Record Writer. "
            + "A null value is skipped and its offset is still committed, unless Preserve Tombstones is true with Output Strategy Use Wrapper or Inject Metadata. "
            + "That writes kafka.tombstone in the record metadata and does not set the FlowFile attribute.");

    private final String description;

    ProcessingStrategy(final String description) {
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }
}

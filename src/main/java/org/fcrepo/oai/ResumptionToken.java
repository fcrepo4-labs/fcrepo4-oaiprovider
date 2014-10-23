/**
 * Copyright 2014 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.oai;

/**
 * A representation of a OAI compliant resumption token.
 *
 * @author Frank Asseg
 */
public class ResumptionToken {

    private final String verb;

    private final String from;

    private final String until;

    private final int offset;

    private final String set;

    private final String metadataPrefix;

    /**
     * Create a new resumption token with the given OAI parameters
     *
     * @param verb the OAI verb
     * @param metadataPrefix the OAI metadata prefix
     * @param from the first date constraint value
     * @param until the secod date constraint value
     * @param offset indicates the current cursor position for list operations
     * @param set the name of the OAI set
     */
    public ResumptionToken(final String verb, final String metadataPrefix, final String from, final String until,
            final int offset, final String set) {
        this.verb = verb;
        this.from = from;
        this.metadataPrefix = metadataPrefix;
        this.until = until;
        this.offset = offset;
        this.set = set;
    }

    /**
     * Gets metadata prefix.
     *
     * @return the metadata prefix
     */
    public String getMetadataPrefix() {
        return metadataPrefix;
    }

    /**
     * Gets offset.
     *
     * @return the offset
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Gets verb.
     *
     * @return the verb
     */
    public String getVerb() {
        return verb;
    }

    /**
     * Gets from.
     *
     * @return the from
     */
    public String getFrom() {
        return from;
    }

    /**
     * Gets until.
     *
     * @return the until
     */
    public String getUntil() {
        return until;
    }

    /**
     * Gets set.
     *
     * @return the set
     */
    public String getSet() {
        return set;
    }
}

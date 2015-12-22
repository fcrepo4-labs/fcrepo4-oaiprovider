/*
 * Copyright 2015 DuraSpace, Inc.
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
package org.fcrepo.oai.service;

import org.openarchives.oai._2.MetadataFormatType;
import org.openarchives.oai._2.ObjectFactory;

/**
 * Metadata form Representation for OAI Provider
 *
 * @author Frank Asseg
 */
public class MetadataFormat {

    private String prefix;

    private String schemaUrl;

    private String namespace;

    private String propertyName;

    /**
     * Get the property name used for the metadata format
     *
     * @return the property name
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * Get the prefix for this metadata format used in oai representations
     *
     * @return the prefix
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * Get this metadata format's XML schema url
     *
     * @return the URL of the XSD
     */
    public String getSchemaUrl() {
        return schemaUrl;
    }

    /**
     * get the namespace for this metadata format
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Get the metadata format as a OAI schema compliant type used by JAX-B for serialization
     *
     * @return the OAI metadata format type
     */
    public MetadataFormatType asMetadataFormatType() {
        final ObjectFactory objectFactory = new ObjectFactory();
        final MetadataFormatType type = objectFactory.createMetadataFormatType();
        type.setSchema(schemaUrl);
        type.setMetadataNamespace(namespace);
        type.setMetadataPrefix(prefix);
        return type;

    }

    /**
     * Sets prefix.
     *
     * @param prefix the prefix
     */
    public void setPrefix(final String prefix) {
        this.prefix = prefix;
    }

    /**
     * Sets schema url.
     *
     * @param schemaUrl the schema url
     */
    public void setSchemaUrl(final String schemaUrl) {
        this.schemaUrl = schemaUrl;
    }

    /**
     * Sets namespace.
     *
     * @param namespace the namespace
     */
    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    /**
     * Sets property name.
     *
     * @param propertyName the property name
     */
    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }
}

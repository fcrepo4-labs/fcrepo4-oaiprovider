/**
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
package org.fcrepo.oai.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.xml.bind.JAXBElement;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.HttpResponse;
import org.junit.Test;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.VerbType;

public class ListMetadataFormatsIT extends AbstractOAIProviderIT {

    @SuppressWarnings("unchecked")
    @Test
    public void testListAvailableMetadataTypes() throws Exception {
        final HttpResponse resp = getOAIPMHResponse(VerbType.LIST_METADATA_FORMATS.value(), null, null, null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());

        final OAIPMHtype oaipmh =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oaipmh.getError().size());
        assertNotNull(oaipmh.getListMetadataFormats());
        assertEquals(3, oaipmh.getListMetadataFormats().getMetadataFormat().size());
        assertEquals("oai_dc", oaipmh.getListMetadataFormats().getMetadataFormat().get(0).getMetadataPrefix());
        assertEquals("http://www.openarchives.org/OAI/2.0/oai_dc/", oaipmh.getListMetadataFormats()
                .getMetadataFormat().get(0).getMetadataNamespace());
        assertEquals("http://www.openarchives.org/OAI/2.0/oai_dc.xsd", oaipmh.getListMetadataFormats()
                .getMetadataFormat().get(0).getSchema());
        assertNotNull(oaipmh.getRequest());
        assertEquals(VerbType.LIST_METADATA_FORMATS.value(), oaipmh.getRequest().getVerb().value());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testListNonExistingObjectMetadataTypes() throws Exception {
        final HttpResponse resp =
                getOAIPMHResponse(VerbType.LIST_METADATA_FORMATS.value(), "non-existing-pid", "oai_dc", null, null,
                        null);
        assertEquals(200, resp.getStatusLine().getStatusCode());

        final OAIPMHtype oaipmh =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(1, oaipmh.getError().size());
        assertEquals(OAIPMHerrorcodeType.ID_DOES_NOT_EXIST, oaipmh.getError().get(0).getCode());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testListObjectMetadataTypes() throws Exception {
        final String pid = "oai-test-" + RandomStringUtils.randomAlphabetic(8);

        this.createFedoraObject(pid);

        final HttpResponse resp = getOAIPMHResponse(VerbType.LIST_METADATA_FORMATS.value(), pid, null, null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());

        final OAIPMHtype oaipmh =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oaipmh.getError().size());
        assertNotNull(oaipmh.getListMetadataFormats());
        assertEquals(1, oaipmh.getListMetadataFormats().getMetadataFormat().size());
        assertEquals("oai_dc", oaipmh.getListMetadataFormats().getMetadataFormat().get(0).getMetadataPrefix());
        assertEquals("http://www.openarchives.org/OAI/2.0/oai_dc/", oaipmh.getListMetadataFormats()
                .getMetadataFormat().get(0).getMetadataNamespace());
        assertEquals("http://www.openarchives.org/OAI/2.0/oai_dc.xsd", oaipmh.getListMetadataFormats()
                .getMetadataFormat().get(0).getSchema());
        assertNotNull(oaipmh.getRequest());
        assertEquals(VerbType.LIST_METADATA_FORMATS.value(), oaipmh.getRequest().getVerb().value());
    }
}

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

package org.fcrepo.oai.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.xml.bind.JAXBElement;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.HttpResponse;
import org.junit.Test;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.VerbType;

public class GetRecordIT extends AbstractOAIProviderIT {

    @Test
    public void testGetNonExistingObjectRecord() throws Exception {
        HttpResponse resp =
                getOAIPMHResponse(VerbType.GET_RECORD.value(), "non-existing-id", "oai_dc", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oai =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(1, oai.getError().size());
        assertEquals(OAIPMHerrorcodeType.ID_DOES_NOT_EXIST, oai.getError().get(0).getCode());
    }

    @Test
    public void testGetOAIDCRecord() throws Exception {
        String objId = "oai-test-" + RandomStringUtils.randomAlphabetic(8);
        createFedoraObject(objId);
        HttpResponse resp = getOAIPMHResponse(VerbType.GET_RECORD.value(), objId, "oai_dc", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oai =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oai.getError().size());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata().getAny());
        assertTrue(oai.getGetRecord().getRecord().getHeader().getIdentifier().endsWith(objId));
    }

    @Test
    public void testGetOAIPremisRecord() throws Exception {
        String objId = "oai-test-" + RandomStringUtils.randomAlphabetic(8);
        String binaryPath = "premis-binary-" + RandomStringUtils.randomAlphabetic(8);

        createBinaryObject(binaryPath, this.getClass().getClassLoader().getResourceAsStream("test-data/premis.xml"));
        createFedoraObjectWithOaiLink(objId, binaryPath, "http://fedora.info/definitions/v4/config#hasOaiPremisRecord");

        HttpResponse resp = getOAIPMHResponse(VerbType.GET_RECORD.value(), objId, "premis", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oai =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oai.getError().size());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata().getAny());
        assertTrue(oai.getGetRecord().getRecord().getHeader().getIdentifier().endsWith(objId));
    }

    @Test
    public void testGetOAIPremisRecordWithPath() throws Exception {
        String objId = "oai-test-" + RandomStringUtils.randomAlphabetic(8);
        String binaryPath = "foo/bar/premis-binary-" + RandomStringUtils.randomAlphabetic(8);

        createBinaryObject(binaryPath, this.getClass().getClassLoader().getResourceAsStream("test-data/premis.xml"));
        createFedoraObjectWithOaiLink(objId, binaryPath, "http://fedora.info/definitions/v4/config#hasOaiPremisRecord");

        HttpResponse resp = getOAIPMHResponse(VerbType.GET_RECORD.value(), objId, "premis", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oai =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oai.getError().size());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata().getAny());
        assertTrue(oai.getGetRecord().getRecord().getHeader().getIdentifier().endsWith(objId));
    }

    @Test
    public void testGetOAIMarc21Record() throws Exception {
        String objId = "oai-test-" + RandomStringUtils.randomAlphabetic(8);
        String binaryPath = "oai-data/marc21-binary-" + RandomStringUtils.randomAlphabetic(8);

        createBinaryObject(binaryPath, this.getClass().getClassLoader().getResourceAsStream("test-data/marc21.xml"));
        createFedoraObjectWithOaiLink(objId, binaryPath, "http://fedora.info/definitions/v4/config#hasOaiMarc21Record");

        HttpResponse resp = getOAIPMHResponse(VerbType.GET_RECORD.value(), objId, "marc21", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oai =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oai.getError().size());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata().getAny());
        assertTrue(oai.getGetRecord().getRecord().getHeader().getIdentifier().endsWith(objId));
    }

    @Test
    public void testGetOAIMarc21RecordWithObjectPath() throws Exception {
        String objId = "foo/oai-test-" + RandomStringUtils.randomAlphabetic(8);
        String binaryPath = "oai-data/marc21-binary-" + RandomStringUtils.randomAlphabetic(8);

        createBinaryObject(binaryPath, this.getClass().getClassLoader().getResourceAsStream("test-data/marc21.xml"));
        createFedoraObjectWithOaiLink(objId, binaryPath, "http://fedora.info/definitions/v4/config#hasOaiMarc21Record");

        HttpResponse resp = getOAIPMHResponse(VerbType.GET_RECORD.value(), objId, "marc21", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oai =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oai.getError().size());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata());
        assertNotNull(oai.getGetRecord().getRecord().getMetadata().getAny());
        assertTrue(oai.getGetRecord().getRecord().getHeader().getIdentifier().endsWith(objId));
    }
}

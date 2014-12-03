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

import java.io.IOException;
import java.io.StringWriter;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.junit.Test;
import org.openarchives.oai._2.IdentifyType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.VerbType;

public class IdentifyIT extends AbstractOAIProviderIT {

    @Test
    @SuppressWarnings("unchecked")
    public void testIdentify() throws Exception {
        HttpResponse resp = getOAIPMHResponse(VerbType.IDENTIFY.value(), null, null, null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oaipmh =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oaipmh.getError().size());
        assertNotNull(oaipmh.getIdentify());
        assertNotNull(oaipmh.getRequest());
        assertEquals(VerbType.IDENTIFY.value(), oaipmh.getRequest().getVerb().value());
        assertEquals("Fedora 4", oaipmh.getIdentify().getRepositoryName());
        assertEquals(serverAddress, oaipmh.getIdentify().getBaseURL());

        createProperty("oai:repositoryName","This is a name to test the repository");
        createProperty("oai:description","Not a description");
        createProperty("oai:adminEmail","someone@somewhere.org");
        HttpResponse resp2 = getOAIPMHResponse(VerbType.IDENTIFY.value(), null, null, null, null, null);
        assertEquals(200, resp2.getStatusLine().getStatusCode());
        OAIPMHtype oaipmh2 =
            ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp2.getEntity().getContent())).getValue();
        assertEquals(0, oaipmh2.getError().size());
        assertNotNull(oaipmh2.getIdentify());
        assertNotNull(oaipmh2.getRequest());
        assertEquals(VerbType.IDENTIFY.value(), oaipmh2.getRequest().getVerb().value());
        assertEquals("This is a name to test the repository", oaipmh2.getIdentify().getRepositoryName());
        //assertEquals("Not a description ["+getVersion()+"]", oaipmh2.getIdentify().getDescription().get(0).getAny());
        assertEquals("someone@somewhere.org",oaipmh2.getIdentify().getAdminEmail().get(0));
        assertEquals(serverAddress, oaipmh2.getIdentify().getBaseURL());
    }
}

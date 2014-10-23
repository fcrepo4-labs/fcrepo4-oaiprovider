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

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.openarchives.oai._2.IdentifyType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.ObjectFactory;
import org.openarchives.oai._2.SetType;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.xml.bind.*;
import javax.xml.namespace.QName;
import java.io.*;

import static java.lang.Integer.MAX_VALUE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.Response.Status.CREATED;
import static org.apache.http.impl.client.HttpClientBuilder.create;
import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * <p>Abstract AbstractResourceIT class.</p>
 *
 * @author fasseg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/spring-test/test-container.xml")
public abstract class AbstractOAIProviderIT {

    protected Logger logger;

    @Before
    public void setLogger() {
        logger = getLogger(this.getClass());
    }

    protected static final int SERVER_PORT = Integer.parseInt(System
            .getProperty("test.port", "8080"));

    protected static final String HOSTNAME = "localhost";

    protected static final String serverAddress = "http://" + HOSTNAME + ":" +
            SERVER_PORT + "/";

    protected final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();

    protected static HttpClient client;

    protected Unmarshaller unmarshaller;

    protected Marshaller marshaller;

    protected ObjectFactory oaiFactory = new ObjectFactory();

    public AbstractOAIProviderIT() {
        connectionManager.setMaxTotal(MAX_VALUE);
        connectionManager.setDefaultMaxPerRoute(5);
        connectionManager.closeIdleConnections(3, SECONDS);
        client = create().setConnectionManager(connectionManager).build();
        try {
            this.marshaller = JAXBContext.newInstance(IdentifyType.class).createMarshaller();
            this.unmarshaller = JAXBContext.newInstance(OAIPMHtype.class).createUnmarshaller();
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAX-B context");
        }
    }

    protected static HttpPost postObjMethod(final String pid) {
        return new HttpPost(serverAddress + pid);
    }

    protected HttpResponse createDatastream(final String pid, final String dsid, final String content)
            throws IOException {
        logger.trace(
                "Attempting to create datastream for object: {} at datastream ID: {}",
                pid, dsid);
        final HttpResponse response =
                client.execute(putDSMethod(pid, dsid, content));
        assertEquals(CREATED.getStatusCode(), response.getStatusLine().getStatusCode());
        return response;
    }

    protected static HttpPut putDSMethod(final String pid, final String ds,
                                         final String content) throws UnsupportedEncodingException {
        final HttpPut put =
                new HttpPut(serverAddress + pid + "/" + ds);

        put.setEntity(new StringEntity(content));
        return put;
    }

    protected int getStatus(final HttpUriRequest method)
        throws ClientProtocolException, IOException {
        logger.debug("Executing: " + method.getMethod() + " to " +
                method.getURI());
        return client.execute(method).getStatusLine().getStatusCode();
    }

    protected void createFedoraObject(final String pid, final String set) throws IOException {
        final HttpPost post = postObjMethod("/");
        if (pid.length() > 0) {
            post.addHeader("Slug", pid);
        }
        if (set != null && !set.isEmpty()) {
            StringBuilder sparql = new StringBuilder("INSERT {")
                .append("<> ")
                .append("<http://fedora.info/definitions/v4/config#isPartOfOAISet> ")
                .append("\"").append(set).append("\" .")
                .append("} WHERE {}");
            post.setEntity(new StringEntity(sparql.toString()));
            post.addHeader("Content-Type", "application/sparql-update");
        }

        final HttpResponse response = client.execute(post);
        assertEquals(CREATED.getStatusCode(), response.getStatusLine().getStatusCode());
        post.releaseConnection();

    }

    protected void createFedoraObject(final String pid) throws IOException {
        this.createFedoraObject(pid, null);
    }

    public HttpResponse getOAIPMHResponse(String tokenData) throws IOException, JAXBException {
        final StringBuilder url = new StringBuilder(serverAddress)
                .append("/oai?resumptionToken=")
                .append(tokenData);
        HttpGet get = new HttpGet(url.toString());
        return this.client.execute(get);
    }

    @SuppressWarnings("unchecked")
    public HttpResponse getOAIPMHResponse(String verb, String identifier, String metadataPrefix, String from,
                                          String until, String set) throws IOException,
            JAXBException {
        final StringBuilder url = new StringBuilder(serverAddress)
                .append("/oai?verb=")
                .append(verb);

        if (identifier != null && !identifier.isEmpty()) {
            url.append("&identifier=").append(identifier);
        }
        if (metadataPrefix != null && !metadataPrefix.isEmpty()) {
            url.append("&metadataPrefix=").append(metadataPrefix);
        }
        if (from != null && !from.isEmpty()) {
            url.append("&from=").append(from);
        }
        if (until != null && !until.isEmpty()) {
            url.append("&until=").append(until);
        }
        if (set != null && !set.isEmpty()) {
            url.append("&set=").append(set);
        }

        HttpGet get = new HttpGet(url.toString());
        return this.client.execute(get);
    }

    protected void createSet(String setName, String setSpec) throws Exception {
        final ObjectFactory fac = new ObjectFactory();
        SetType set = fac.createSetType();
        set.setSetName(setName);
        if (setSpec != null) {
            set.setSetSpec(setSpec);
        } else {
            set.setSetSpec(setName);
        }
        HttpPost post = new HttpPost(serverAddress + "/oai/sets");
        post.setEntity(new InputStreamEntity(toStream(set), ContentType.TEXT_XML));
        HttpResponse resp = this.client.execute(post);
        assertEquals(201, resp.getStatusLine().getStatusCode());
    }

    private InputStream toStream(SetType set) throws JAXBException {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        marshaller.marshal(new JAXBElement(new QName("set"), SetType.class, set), sink);
        return new ByteArrayInputStream(sink.toByteArray());
    }
}

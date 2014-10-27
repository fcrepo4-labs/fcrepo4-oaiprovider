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

package org.fcrepo.oai.http;

import static org.openarchives.oai._2.VerbType.GET_RECORD;
import static org.openarchives.oai._2.VerbType.IDENTIFY;
import static org.openarchives.oai._2.VerbType.LIST_IDENTIFIERS;
import static org.openarchives.oai._2.VerbType.LIST_METADATA_FORMATS;
import static org.openarchives.oai._2.VerbType.LIST_RECORDS;
import static org.openarchives.oai._2.VerbType.LIST_SETS;

import java.io.InputStream;
import java.net.URI;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBException;

import org.fcrepo.oai.service.OAIProviderService;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;

/**
 * The type OAI web resource.
 *
 * @author Frank Asseg
 */
@Scope("request")
@Path("/oai")
public class OAIWebResource {

    @Inject
    private Session session;

    @Autowired
    private OAIProviderService providerService;

    /**
     * Create set.
     *
     * @param uriInfo the uri info
     * @param src the src
     * @return the response
     * @throws RepositoryException the repository exception
     */
    @POST
    @Path("/sets")
    @Consumes(MediaType.TEXT_XML)
    public Response createSet(@Context final UriInfo uriInfo, final InputStream src) throws RepositoryException {
        final String path = this.providerService.createSet(session, uriInfo, src);
        return Response.created(URI.create(path)).build();
    }

    /**
     * Gets OAI response.
     *
     * @param verbParam the verb
     * @param identifierParam the identifier
     * @param metadataPrefixParam the metadata prefix
     * @param fromParam the from
     * @param untilParam the until
     * @param setParam the set
     * @param resumptionToken the resumption token
     * @param uriInfo the uri info
     * @return the oAI response
     * @throws RepositoryException the repository exception
     */
    @GET
    @Produces(MediaType.TEXT_XML)
    public Object getOAIResponse(
            final @QueryParam("verb") String verbParam, final @QueryParam("identifier") String identifierParam,
            final @QueryParam("metadataPrefix") String metadataPrefixParam, final @QueryParam("from") String fromParam,
            final @QueryParam("until") String untilParam, final @QueryParam("set") String setParam,
            final @QueryParam("resumptionToken") String resumptionToken, final @Context UriInfo uriInfo)
            throws RepositoryException {

        int offset = 0;

        final String verb;
        final String from;
        final String until;
        final String set;
        final String metadataPrefix;
        final String identifier;

        if (resumptionToken != null && !resumptionToken.isEmpty()) {
            /* If there's a resumption token present the data provided in the
                base64 encoded token is used to generate the request */
            try {
                final ResumptionToken token = OAIProviderService.decodeResumptionToken(resumptionToken);
                identifier = null;
                verb = token.getVerb();
                from = token.getFrom();
                until = token.getUntil();
                set = token.getSet();
                metadataPrefix = token.getMetadataPrefix();
                offset = token.getOffset();
            } catch (Exception e) {
                return providerService.error(null, null, null, OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN,
                        "Resumption token is invalid");
            }
        } else {
            /* otherwise just read the query params */
            identifier = identifierParam;
            verb = verbParam;
            from = fromParam;
            until = untilParam;
            set = setParam;
            metadataPrefix = metadataPrefixParam;
        }

        /* decide what to do depending on the verb passed */
        if (verb == null) {
            return providerService.error(null, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT,
                    "Verb is required");
        }

        /* identify response */
        if (verb.equals(IDENTIFY.value())) {
            try {
                verifyEmpty(identifier, metadataPrefix, from, until, set);
                return providerService.identify(this.session, uriInfo);
            } catch (JAXBException | IllegalArgumentException e) {
                return providerService.error(VerbType.IDENTIFY, identifier, metadataPrefix,
                        OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
        }

        /* ListMetadataFormats response */
        if (verb.equals(LIST_METADATA_FORMATS.value())) {
            try {
                verifyEmpty(from, until, set);
                return providerService.listMetadataFormats(this.session, uriInfo, identifier);
            } catch (IllegalArgumentException e) {
                return providerService.error(VerbType.LIST_METADATA_FORMATS, identifier, metadataPrefix,
                        OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
        }

        /* GetRecord response */
        if (verb.equals(GET_RECORD.value())) {
            try {
                verifyEmpty(from, until, set);
                return providerService.getRecord(this.session, uriInfo, identifier, metadataPrefix);

            } catch (IllegalArgumentException e) {
                return providerService.error(VerbType.GET_RECORD, identifier, metadataPrefix,
                        OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
        }

        /* list identifiers response */
        if (verb.equals(LIST_IDENTIFIERS.value())) {
            try {
                verifyEmpty(identifier);
                return providerService.listIdentifiers(this.session, uriInfo, metadataPrefix, from, until, set, offset);
            } catch (IllegalArgumentException e) {
                return providerService.error(VerbType.LIST_IDENTIFIERS, identifier, metadataPrefix,
                        OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
        }

        /* list sets response */
        if (verb.equals(LIST_SETS.value())) {
            try {
                verifyEmpty(identifier);
            } catch (IllegalArgumentException e) {
                return providerService.error(VerbType.LIST_SETS, identifier, metadataPrefix,
                        OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
            return providerService.listSets(session, uriInfo, offset);
        }

        /* list records response */
        if (verb.equals(LIST_RECORDS.value())) {
            try {
                verifyEmpty(identifier);
                return providerService.listRecords(this.session, uriInfo, metadataPrefix, from, until, set, offset);
            } catch (IllegalArgumentException e) {
                return providerService.error(VerbType.LIST_SETS, identifier, metadataPrefix,
                        OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
        }
        return providerService.error(null, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_VERB,
                "Unknown verb '" + verb + "'");
    }

    private void verifyEmpty(final String... data) throws IllegalArgumentException {
        for (String s : data) {
            if (s != null && !s.isEmpty()) {
                throw new IllegalArgumentException("Wrong argument for method");
            }
        }
    }

}

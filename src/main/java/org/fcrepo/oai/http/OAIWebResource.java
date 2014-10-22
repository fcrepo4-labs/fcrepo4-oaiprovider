/* 
 * Copyright 2014 Frank Asseg
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */

package org.fcrepo.oai.http;

import org.fcrepo.oai.service.OAIProviderService;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBException;
import java.io.InputStream;

import static org.openarchives.oai._2.VerbType.*;

@Scope("request")
@Path("/oai")
public class OAIWebResource {

    @Inject
    private Session session;

    @Autowired
    private OAIProviderService providerService;

    @POST
    @Path("/sets")
    @Consumes(MediaType.TEXT_XML)
    public Response createSet(@Context final UriInfo uriInfo, final InputStream src) throws RepositoryException {
        return Response.serverError().build();
    }

    @GET
    @Produces(MediaType.TEXT_XML)
    public Object getOAIResponse(
            @QueryParam("verb") String verb,
            @QueryParam("identifier") final String identifier,
            @QueryParam("metadataPrefix") String metadataPrefix,
            @QueryParam("from") String from,
            @QueryParam("until") String until,
            @QueryParam("set") String set,
            @QueryParam("resumptionToken") final String resumptionToken,
            @Context final UriInfo uriInfo) throws RepositoryException {
        int offset = 0;


//        /* If there's a resumption token present the data provided in the base64 encoded token is used to generate the request */
//        if (resumptionToken != null && !resumptionToken.isEmpty()) {
//            try {
//                final ResumptionToken token = OAIProviderService.decodeResumptionToken(resumptionToken);
//                verb = token.getVerb();
//                from = token.getFrom();
//                until = token.getUntil();
//                set = token.getSet();
//                metadataPrefix = token.getMetadataPrefix();
//                offset = token.getOffset();
//            } catch (Exception e) {
//                return providerService.error(null, null, null, OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN, "Resumption token is invalid");
//            }
//        }

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
                return providerService.error(VerbType.IDENTIFY, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
        }

        /* ListMetadataFormats response */
        if (verb.equals(LIST_METADATA_FORMATS.value())) {
            try {
                verifyEmpty(from, until, set);
            } catch (IllegalArgumentException e) {
                return providerService.error(VerbType.LIST_METADATA_FORMATS, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
            return providerService.listMetadataFormats(this.session, uriInfo, identifier);
        }

        /* GetRecord response */
        if (verb.equals(GET_RECORD.value())) {
            try {
                verifyEmpty(from, until, set);
            } catch (IllegalArgumentException e) {
                return providerService.error(VerbType.GET_RECORD, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
            }
            return providerService.getRecord(this.session, uriInfo, identifier, metadataPrefix);
        }
        return providerService.error(null, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_VERB, "Unknown verb '" + verb + "'");

//        } else if (verb.equals(LIST_IDENTIFIERS.value())) {
//            try {
//                verifyEmpty(identifier);
//            }catch(IllegalArgumentException e) {
//                return providerService.error(VerbType.LIST_IDENTIFIERS, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
//            }
//            return providerService.listIdentifiers(this.session, uriInfo, metadataPrefix, from, until, set, offset);
//        } else if (verb.equals(LIST_SETS.value())) {
//            try {
//                verifyEmpty(identifier);
//            }catch(IllegalArgumentException e) {
//                return providerService.error(VerbType.LIST_SETS, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
//            }
//            return providerService.listSets(session, uriInfo, offset);
//        } else if (verb.equals(LIST_RECORDS.value())) {
//            try {
//                verifyEmpty(identifier);
//            }catch(IllegalArgumentException e) {
//                return providerService.error(VerbType.LIST_SETS, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT, "Invalid arguments");
//            }
//            return providerService.listRecords(this.session, uriInfo, metadataPrefix, from, until, set, offset);
//        } else {
//            return providerService.error(null, identifier, metadataPrefix, OAIPMHerrorcodeType.BAD_VERB,
//                    "The verb '" + verb + "' is invalid");
//        }
    }

    private void verifyEmpty(String ... data) throws IllegalArgumentException{
        for (String s:data) {
            if (s != null && !s.isEmpty())  {
                throw new IllegalArgumentException("Wrong argument for method");
            }
        }
    }


}

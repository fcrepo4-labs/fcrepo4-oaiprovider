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

import org.fcrepo.oai.ResumptionToken;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import java.io.InputStream;
import java.net.URI;

import static org.openarchives.oai._2.VerbType.*;

@Component
@Scope("prototype")
@Path("/oai")
public class OAIWebResource {

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
        return Response.serverError().build();
    }

}

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
package org.fcrepo.oai.dublincore;

import com.hp.hpl.jena.graph.Triple;
import org.fcrepo.http.api.FedoraNodes;
import org.fcrepo.http.commons.api.rdf.HttpResourceConverter;
import org.fcrepo.kernel.FedoraObject;
import org.fcrepo.kernel.impl.rdf.impl.PropertiesRdfContext;
import org.fcrepo.kernel.utils.iterators.RdfStream;
import org.openarchives.oai._2_0.oai_dc.OaiDcType;
import org.purl.dc.elements._1.ElementType;
import org.purl.dc.elements._1.ObjectFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBElement;

/**
 * The type Jcr properties generator.
 *
 * @author Frank Asseg
 */
public class JcrPropertiesGenerator {
    private static final ObjectFactory dcFactory = new ObjectFactory();
    private static final org.openarchives.oai._2_0.oai_dc.ObjectFactory oaiDcFactory =
            new org.openarchives.oai._2_0.oai_dc.ObjectFactory();


    /**
     * Generate dC.
     *
     * @param obj the obj
     * @return the jAXB element
     */
    public JAXBElement<OaiDcType> generateDC(final Session session, final FedoraObject obj, final UriInfo uriInfo)
            throws RepositoryException {

        final HttpResourceConverter converter = new HttpResourceConverter(session, uriInfo.getBaseUriBuilder()
                .clone().path(FedoraNodes.class));
        final OaiDcType oaidc = this.oaiDcFactory.createOaiDcType();

        final ElementType valId = this.dcFactory.createElementType();
        valId.setValue(converter.toDomain(obj.getPath()).getURI());
        oaidc.getTitleOrCreatorOrSubject().add(this.dcFactory.createIdentifier(valId));

        final ElementType valCreated = this.dcFactory.createElementType();
        valCreated.setValue(obj.getCreatedDate().toString());
        oaidc.getTitleOrCreatorOrSubject().add(this.dcFactory.createDate(valCreated));

        final ElementType valCreator = this.dcFactory.createElementType();
        valCreator.setValue(obj.getProperty("jcr:createdBy").getValue().getString());
        oaidc.getTitleOrCreatorOrSubject().add(this.dcFactory.createCreator(valCreator));

        final RdfStream triples = obj.getTriples(converter, PropertiesRdfContext.class);
        while (triples.hasNext()) {
            final ElementType valRel = this.dcFactory.createElementType();
            final Triple triple = triples.next();
            valRel.setValue(triple.getPredicate().toString() + " " + triple.getObject().toString());
            oaidc.getTitleOrCreatorOrSubject().add(this.dcFactory.createRelation(valRel));
        }

        oaidc.getTitleOrCreatorOrSubject().add(this.dcFactory.createSubject(valId));

        return this.oaiDcFactory.createDc(oaidc);
    }
}

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

package org.fcrepo.oai.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.fcrepo.generator.dublincore.JcrPropertiesGenerator;
import org.fcrepo.http.api.FedoraNodes;
import org.fcrepo.http.commons.api.rdf.HttpResourceConverter;
import org.fcrepo.http.commons.session.SessionFactory;
import org.fcrepo.kernel.FedoraBinary;
import org.fcrepo.kernel.FedoraObject;
import org.fcrepo.kernel.RdfLexicon;
import org.fcrepo.kernel.impl.rdf.impl.PropertiesRdfContext;
import org.fcrepo.kernel.services.BinaryService;
import org.fcrepo.kernel.services.NodeService;
import org.fcrepo.kernel.services.ObjectService;
import org.fcrepo.kernel.utils.iterators.RdfStream;
import org.fcrepo.oai.MetadataFormat;
import org.fcrepo.oai.PropertyPredicate;
import org.fcrepo.oai.ResumptionToken;
import org.fcrepo.transform.sparql.JQLConverter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.openarchives.oai._2.DescriptionType;
import org.openarchives.oai._2.GetRecordType;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.IdentifyType;
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.ListMetadataFormatsType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.ListSetsType;
import org.openarchives.oai._2.MetadataFormatType;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.ObjectFactory;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.RequestType;
import org.openarchives.oai._2.SetType;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * The type OAI provider service.
 *
 * @author Frank Asseg
 */
public class OAIProviderService {

    private static final ObjectFactory oaiFactory = new ObjectFactory();

    private final DatatypeFactory dataFactory;

    private final Unmarshaller unmarshaller;

    private final Model rdfModel = ModelFactory.createDefaultModel();

    private String identifyPath;

    private String setsRootPath;

    private String propertyHasSets;

    private String propertySetName;

    private String propertyHasSetSpec;

    private String propertyIsPartOfSet;

    private boolean setsEnabled;

    private boolean autoGenerateOaiDc;

    private Map<String, MetadataFormat> metadataFormats;

    private DateTimeFormatter dateFormat = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC);

    private int maxListSize;

    @Autowired
    private BinaryService binaryService;

    @Autowired
    private NodeService nodeService;

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private ObjectService objectService;

    @Autowired
    private JcrPropertiesGenerator jcrPropertiesGenerator;

    /**
     * Sets property has set spec.
     *
     * @param propertyHasSetSpec the property has set spec
     */
    public void setPropertyHasSetSpec(String propertyHasSetSpec) {
        this.propertyHasSetSpec = propertyHasSetSpec;
    }

    /**
     * Sets property set name.
     *
     * @param propertySetName the property set name
     */
    public void setPropertySetName(String propertySetName) {
        this.propertySetName = propertySetName;
    }

    /**
     * Sets property has sets.
     *
     * @param propertyHasSets the property has sets
     */
    public void setPropertyHasSets(String propertyHasSets) {
        this.propertyHasSets = propertyHasSets;
    }

    /**
     * Sets max list size.
     *
     * @param maxListSize the max list size
     */
    public void setMaxListSize(int maxListSize) {
        this.maxListSize = maxListSize;
    }

    /**
     * Sets property is part of set.
     *
     * @param propertyIsPartOfSet the property is part of set
     */
    public void setPropertyIsPartOfSet(String propertyIsPartOfSet) {
        this.propertyIsPartOfSet = propertyIsPartOfSet;
    }

    /**
     * Sets sets root path.
     *
     * @param setsRootPath the sets root path
     */
    public void setSetsRootPath(String setsRootPath) {
        this.setsRootPath = setsRootPath;
    }

    /**
     * Sets sets enabled.
     *
     * @param setsEnabled the sets enabled
     */
    public void setSetsEnabled(boolean setsEnabled) {
        this.setsEnabled = setsEnabled;
    }

    /**
     * Sets identify path.
     *
     * @param identifyPath the identify path
     */
    public void setIdentifyPath(String identifyPath) {
        this.identifyPath = identifyPath;
    }

    /**
     * Sets auto generate oai dc.
     *
     * @param autoGenerateOaiDc the auto generate oai dc
     */
    public void setAutoGenerateOaiDc(boolean autoGenerateOaiDc) {
        this.autoGenerateOaiDc = autoGenerateOaiDc;
    }

    /**
     * Sets metadata formats.
     *
     * @param metadataFormats the metadata formats
     */
    public void setMetadataFormats(Map<String, MetadataFormat> metadataFormats) {
        this.metadataFormats = metadataFormats;
    }

    /**
     * Service intitialization
     *
     * @throws RepositoryException the repository exception
     */
    @PostConstruct
    public void init() throws RepositoryException {
        /* check if set root node exists */
        Session session = sessionFactory.getInternalSession();
        if (!this.nodeService.exists(session, setsRootPath)) {
            this.objectService.findOrCreateObject(session, setsRootPath);
        }
        session.save();
    }

    /**
     * Instantiates a new OAI provider service.
     *
     * @throws DatatypeConfigurationException the datatype configuration exception
     * @throws JAXBException the jAXB exception
     */
    public OAIProviderService() throws DatatypeConfigurationException, JAXBException {
        this.dataFactory = DatatypeFactory.newInstance();
        final JAXBContext ctx = JAXBContext.newInstance(OAIPMHtype.class, IdentifyType.class, SetType.class);
        this.unmarshaller = ctx.createUnmarshaller();
    }

    /**
     * Identify jAXB element.
     *
     * @param session the session
     * @param uriInfo the uri info
     * @return the jAXB element
     * @throws RepositoryException the repository exception
     * @throws JAXBException the jAXB exception
     */
    public JAXBElement<OAIPMHtype> identify(final Session session, UriInfo uriInfo) throws RepositoryException,
            JAXBException {
        if (!this.nodeService.exists(session, identifyPath)) {
            return error(VerbType.IDENTIFY, null, null, OAIPMHerrorcodeType.BAD_ARGUMENT,
                    "Identify response does not exists");
        }
        final FedoraBinary binary = this.binaryService.findOrCreateBinary(session, identifyPath);
        final InputStream data = binary.getContent();
        final IdentifyType id = this.unmarshaller.unmarshal(new StreamSource(data), IdentifyType.class).getValue();

        final RequestType req = oaiFactory.createRequestType();
        req.setVerb(VerbType.IDENTIFY);
        req.setValue(uriInfo.getRequestUri().toASCIIString());

        final OAIPMHtype oai = oaiFactory.createOAIPMHtype();
        oai.setIdentify(id);
        oai.setResponseDate(dataFactory.newXMLGregorianCalendar(new GregorianCalendar()));
        oai.setRequest(req);
        return oaiFactory.createOAIPMH(oai);
    }

    /**
     * List metadata formats.
     *
     * @param session the session
     * @param uriInfo the uri info
     * @param identifier the identifier
     * @return the jAXB element
     * @throws RepositoryException the repository exception
     */
    public JAXBElement<OAIPMHtype> listMetadataFormats(final Session session, final UriInfo uriInfo,
            final String identifier) throws RepositoryException {

        final ListMetadataFormatsType listMetadataFormats = oaiFactory.createListMetadataFormatsType();

        /* check which formats are available on top of oai_dc for this object */
        if (identifier != null && !identifier.isEmpty()) {
            final String path = "/" + identifier;
            if (path != null && !path.isEmpty()) {
                /* generate metadata format response for a single pid */
                if (!this.nodeService.exists(session, path)) {
                    return error(VerbType.LIST_METADATA_FORMATS, identifier, null,
                            OAIPMHerrorcodeType.ID_DOES_NOT_EXIST,
                            "The object does not exist");
                }
                final FedoraObject obj = this.objectService.findOrCreateObject(session, "/" + identifier);
                for (MetadataFormat mdf : metadataFormats.values()) {
                    if (mdf.getPrefix().equals("oai_dc") || obj.hasProperty(mdf.getPropertyName())) {
                        listMetadataFormats.getMetadataFormat().add(mdf.asMetadataFormatType());
                    }
                }
            }
        } else {
            /* generate a general metadata format response */
            listMetadataFormats.getMetadataFormat().addAll(listAvailableMetadataFormats());
        }

        final RequestType req = oaiFactory.createRequestType();
        req.setVerb(VerbType.LIST_METADATA_FORMATS);
        req.setValue(uriInfo.getRequestUri().toASCIIString());

        final OAIPMHtype oai = oaiFactory.createOAIPMHtype();
        oai.setListMetadataFormats(listMetadataFormats);
        oai.setRequest(req);
        return oaiFactory.createOAIPMH(oai);
    }

    private List<MetadataFormatType> listAvailableMetadataFormats() {
        final List<MetadataFormatType> types = new ArrayList<>(metadataFormats.size());
        for (MetadataFormat mdf : metadataFormats.values()) {
            final MetadataFormatType mdft = oaiFactory.createMetadataFormatType();
            mdft.setMetadataPrefix(mdf.getPrefix());
            mdft.setMetadataNamespace(mdf.getNamespace());
            mdft.setSchema(mdf.getSchemaUrl());
            types.add(mdft);
        }
        return types;
    }

    /**
     * Gets record.
     *
     * @param session the session
     * @param uriInfo the uri info
     * @param identifier the identifier
     * @param metadataPrefix the metadata prefix
     * @return the record
     * @throws RepositoryException the repository exception
     */
    public JAXBElement<OAIPMHtype> getRecord(final Session session, final UriInfo uriInfo, final String identifier,
            final String metadataPrefix) throws RepositoryException {
        final MetadataFormat format = metadataFormats.get(metadataPrefix);
        if (format == null) {
            return error(VerbType.GET_RECORD, identifier, metadataPrefix,
                    OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT, "The metadata format is not available");
        }

        final String path = "/" + identifier;
        if (!this.nodeService.exists(session, path)) {
            return error(VerbType.GET_RECORD, identifier, metadataPrefix, OAIPMHerrorcodeType.ID_DOES_NOT_EXIST,
                    "The requested identifier does not exist");
        }

        if (format.getPrefix().equals("oai_dc")) {
            /* generate a OAI DC reponse using the DC Generator from fcrepo4 */
            return generateOaiDc(session, identifier, uriInfo);
        } else {
            /* generate a OAI response from the linked Binary */
            return fetchOaiResponse(session, identifier, format);
        }
    }

    private JAXBElement<OAIPMHtype> generateOaiDc(final Session session, String identifier, UriInfo uriInfo)
            throws RepositoryException {
        final FedoraObject obj = this.objectService.findOrCreateObject(session, "/" + identifier);

        final OAIPMHtype oai = oaiFactory.createOAIPMHtype();
        final RequestType req = oaiFactory.createRequestType();
        req.setVerb(VerbType.GET_RECORD);
        req.setValue(uriInfo.getRequestUri().toASCIIString());
        oai.setRequest(req);

        final GetRecordType getRecord = oaiFactory.createGetRecordType();
        final RecordType record = oaiFactory.createRecordType();
        getRecord.setRecord(record);

        final HeaderType header = oaiFactory.createHeaderType();
        header.setIdentifier(identifier);
        header.setDatestamp(dateFormat.print(new Date().getTime()));
        record.setHeader(header);

        final MetadataType md = this.oaiFactory.createMetadataType();
        final InputStream src = jcrPropertiesGenerator.getStream(obj.getNode());
        if (src == null) {
            return error(VerbType.GET_RECORD, identifier, "oai_dc", OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT,
                    "Error occured while trying to generate Dublin Core response");
        }
        try {
            md.setAny(new JAXBElement<String>(new QName("oai_dc"), String.class, IOUtils.toString(src)));
        } catch (IOException e) {
            throw new RepositoryException(e);
        } finally {
            IOUtils.closeQuietly(src);
        }
        record.setMetadata(md);
        oai.setGetRecord(getRecord);

        return this.oaiFactory.createOAIPMH(oai);
    }

    private JAXBElement<OAIPMHtype> fetchOaiResponse(Session session, String identifier, MetadataFormat format) {
        return null;
    }

    /**
     * Creates a OAI error response for JAX-B
     *
     * @param verb the verb
     * @param identifier the identifier
     * @param metadataPrefix the metadata prefix
     * @param errorCode the error code
     * @param msg the msg
     * @return the jAXB element
     */
    public static JAXBElement<OAIPMHtype> error(VerbType verb, String identifier, String metadataPrefix,
            OAIPMHerrorcodeType errorCode, String msg) {
        final OAIPMHtype oai = oaiFactory.createOAIPMHtype();
        final RequestType req = oaiFactory.createRequestType();
        req.setVerb(verb);
        req.setIdentifier(identifier);
        req.setMetadataPrefix(metadataPrefix);
        oai.setRequest(req);

        final OAIPMHerrorType error = oaiFactory.createOAIPMHerrorType();
        error.setCode(errorCode);
        error.setValue(msg);
        oai.getError().add(error);
        return oaiFactory.createOAIPMH(oai);
    }

    /**
     * List identifiers.
     *
     * @param session the session
     * @param uriInfo the uri info
     * @param metadataPrefix the metadata prefix
     * @param from the from
     * @param until the until
     * @param set the set
     * @param offset the offset
     * @return the jAXB element
     * @throws RepositoryException the repository exception
     */
    public JAXBElement<OAIPMHtype> listIdentifiers(Session session, UriInfo uriInfo, String metadataPrefix,
                                                   String from, String until, String set, int offset)
            throws RepositoryException {

        if (metadataPrefix == null) {
            return error(VerbType.LIST_IDENTIFIERS, null, null, OAIPMHerrorcodeType.BAD_ARGUMENT,
                    "metadataprefix is invalid");
        }

        final MetadataFormat mdf = metadataFormats.get(metadataPrefix);
        if (mdf == null) {
            return error(VerbType.LIST_IDENTIFIERS, null, metadataPrefix,
                    OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT, "Unavailable metadata format");
        }

        DateTime fromDateTime = null;
        DateTime untilDateTime = null;
        try {
            fromDateTime = (from != null && !from.isEmpty()) ? dateFormat.parseDateTime(from) : null;
            untilDateTime = (until != null && !until.isEmpty()) ? dateFormat.parseDateTime(until) : null;
        } catch (IllegalArgumentException e) {
            return error(VerbType.LIST_IDENTIFIERS, null, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT,
                    e.getMessage());
        }

        final StringBuilder sparql =
                new StringBuilder("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ")
                        .append("SELECT ?sub ?pred ?obj WHERE {?sub <" +
                                RdfLexicon.HAS_MIXIN_TYPE + "> \"fedora:object\" . ");

        final List<String> filters = new ArrayList<>();

        if (fromDateTime != null || untilDateTime != null) {
            sparql.append("?sub <").append(RdfLexicon.LAST_MODIFIED_DATE).append("> ?date . ");
            if (fromDateTime != null) {
                filters.add("?date >='" + from + "'^^xsd:dateTime ");
            }
            if (untilDateTime != null) {
                filters.add("?date <='" + until + "'^^xsd:dateTime ");
            }
        }

        if (set != null && !set.isEmpty()) {
            if (!setsEnabled) {
                return error(VerbType.LIST_IDENTIFIERS, null, metadataPrefix, OAIPMHerrorcodeType.NO_SET_HIERARCHY,
                        "Sets are not enabled");
            }
            sparql.append("?sub <").append(propertyIsPartOfSet).append("> ?set . ");
            filters.add("?set = '" + set + "'");
        }

        int filterCount = 0;
        for (String filter : filters) {
            if (filterCount++ == 0) {
                sparql.append("FILTER (");
            }
            sparql.append(filter).append(filterCount == filters.size() ? ")" : " && ");
        }
        sparql.append("}")
                .append(" OFFSET ").append(offset)
                .append(" LIMIT ").append(maxListSize);

        final HttpResourceConverter converter = new HttpResourceConverter(session,
                uriInfo.getBaseUriBuilder().clone().path(FedoraNodes.class));

        try {
            final JQLConverter jql = new JQLConverter(session, converter, sparql.toString());
            final ResultSet result = jql.execute();
            final OAIPMHtype oai = oaiFactory.createOAIPMHtype();
            final ListIdentifiersType ids = oaiFactory.createListIdentifiersType();
            if (!result.hasNext()) {
                return error(VerbType.LIST_IDENTIFIERS, null, metadataPrefix, OAIPMHerrorcodeType.NO_RECORDS_MATCH,
                        "No record found");
            }
            while (result.hasNext()) {
                final HeaderType h = oaiFactory.createHeaderType();
                final QuerySolution sol = result.next();
                final Resource sub = sol.get("sub").asResource();
                final String path = converter.convert(sub).getPath();

                h.setIdentifier(sub.getURI());
                final FedoraObject obj =
                        this.objectService.findOrCreateObject(session, path);
                h.setDatestamp(dateFormat.print(obj.getLastModifiedDate().getTime()));


                RdfStream triples = obj.getTriples(converter, PropertiesRdfContext.class).filter(
                        new PropertyPredicate(propertyIsPartOfSet));
                final List<String> setNames = new ArrayList<>();
                while (triples.hasNext()) {
                    setNames.add(triples.next().getObject().getLiteralValue().toString());
                }
                for (String name : setNames) {
                    final FedoraObject setObject = this.objectService.findOrCreateObject(session, setsRootPath + "/"
                            + name);
                    final RdfStream setTriples = setObject.getTriples(converter, PropertiesRdfContext.class).filter(
                            new PropertyPredicate(propertyHasSetSpec));
                    h.getSetSpec().add(setTriples.next().getObject().getLiteralValue().toString());
                }
                ids.getHeader().add(h);
            }

            final RequestType req = oaiFactory.createRequestType();
            if (ids.getHeader().size() == maxListSize) {
                req.setResumptionToken(encodeResumptionToken(VerbType.LIST_IDENTIFIERS.value(), metadataPrefix, from,
                        until, set,
                        offset + maxListSize));
            }
            req.setVerb(VerbType.LIST_IDENTIFIERS);
            req.setMetadataPrefix(metadataPrefix);
            oai.setRequest(req);
            oai.setListIdentifiers(ids);
            return oaiFactory.createOAIPMH(oai);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RepositoryException(e);
        }
    }

    /**
     * Encode resumption token.
     *
     * @param verb the verb
     * @param metadataPrefix the metadata prefix
     * @param from the from
     * @param until the until
     * @param set the set
     * @param offset the offset
     * @return the string
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    public static String encodeResumptionToken(String verb, String metadataPrefix, String from, String until,
            String set, int offset) throws UnsupportedEncodingException {
        if (from == null) {
            from = "";
        }
        if (until == null) {
            until = "";
        }
        if (set == null) {
            set = "";
        }
        String[] data = new String[] {
            urlEncode(verb),
            urlEncode(metadataPrefix),
            urlEncode(from),
            urlEncode(until),
            urlEncode(set),
            urlEncode(String.valueOf(offset))
        };
        return Base64.encodeBase64URLSafeString(StringUtils.join(data, ':').getBytes("UTF-8"));
    }

    /**
     * Url encode.
     *
     * @param value the value
     * @return the string
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    public static String urlEncode(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, "UTF-8");
    }

    /**
     * Url decode.
     *
     * @param value the value
     * @return the string
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    public static String urlDecode(String value) throws UnsupportedEncodingException {
        return URLDecoder.decode(value, "UTF-8");
    }

    /**
     * Decode resumption token.
     *
     * @param token the token
     * @return the resumption token
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    public static ResumptionToken decodeResumptionToken(String token) throws UnsupportedEncodingException {
        String[] data = StringUtils.splitPreserveAllTokens(new String(Base64.decodeBase64(token)), ':');
        final String verb = urlDecode(data[0]);
        final String metadataPrefix = urlDecode(data[1]);
        final String from = urlDecode(data[2]);
        final String until = urlDecode(data[3]);
        final String set = urlDecode(data[4]);
        final int offset = Integer.parseInt(urlDecode(data[5]));
        return new ResumptionToken(verb, metadataPrefix, from, until, offset, set);
    }

    /**
     * List sets.
     *
     * @param session the session
     * @param uriInfo the uri info
     * @param offset the offset
     * @return the jAXB element
     * @throws RepositoryException the repository exception
     */
    public JAXBElement<OAIPMHtype> listSets(Session session, UriInfo uriInfo, int offset) throws RepositoryException {
        final HttpResourceConverter converter =
                new HttpResourceConverter(session, uriInfo.getBaseUriBuilder().clone().path(FedoraNodes.class));
        try {
            if (!setsEnabled) {
                return error(VerbType.LIST_SETS, null, null, OAIPMHerrorcodeType.NO_SET_HIERARCHY,
                        "Set are not enabled");
            }
            final StringBuilder sparql = new StringBuilder("SELECT ?obj WHERE {")
                    .append("<").append(converter.toDomain(setsRootPath)).append(">")
                    .append("<").append(propertyHasSets).append("> ?obj }");
            final JQLConverter jql = new JQLConverter(session, converter, sparql.toString());
            final ResultSet result = jql.execute();
            final OAIPMHtype oai = oaiFactory.createOAIPMHtype();
            final ListSetsType sets = oaiFactory.createListSetsType();
            while (result.hasNext()) {
                final Resource setRes = result.next().get("obj").asResource();
                sparql.setLength(0);
                sparql.append("SELECT ?name ?spec WHERE {")
                        .append("<").append(setRes).append("> ")
                        .append("<").append(propertySetName).append("> ")
                        .append("?name ; ")
                        .append("<").append(propertyHasSetSpec).append("> ")
                        .append("?spec . ")
                        .append("}");
                final JQLConverter setJql = new JQLConverter(session, converter, sparql.toString());
                final ResultSet setResult = setJql.execute();
                while (setResult.hasNext()) {
                    final SetType set = oaiFactory.createSetType();
                    QuerySolution sol = setResult.next();
                    set.setSetName(sol.get("name").asLiteral().getString());
                    set.setSetSpec(sol.get("spec").asLiteral().getString());
                    sets.getSet().add(set);
                }
            }
            oai.setListSets(sets);
            return oaiFactory.createOAIPMH(oai);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RepositoryException(e);
        }
    }

    /**
     * Create set.
     *
     * @param session the session
     * @param uriInfo the uri info
     * @param src the src
     * @return the string
     * @throws RepositoryException the repository exception
     */
    public String createSet(Session session, UriInfo uriInfo, InputStream src) throws RepositoryException {
        final HttpResourceConverter converter =
                new HttpResourceConverter(session, uriInfo.getBaseUriBuilder().clone().path(FedoraNodes.class));
        try {
            final SetType set = this.unmarshaller.unmarshal(new StreamSource(src), SetType.class).getValue();
            final String setId = getSetId(set);
            if (!this.nodeService.exists(session, setsRootPath)) {
                throw new RepositoryException("The root set object does not exist");
            }
            final FedoraObject setRoot = this.objectService.findOrCreateObject(session, setsRootPath);
            if (set.getSetSpec() != null) {
                /* validate that the hierarchy of sets exists */
            }

            if (this.nodeService.exists(session, setsRootPath + "/" + setId)) {
                throw new RepositoryException("The OAI Set with the id already exists");
            }
            final FedoraObject setObject = this.objectService.findOrCreateObject(session, setsRootPath + "/" + setId);

            StringBuilder sparql =
                    new StringBuilder("INSERT DATA {<" + converter.toDomain(setRoot.getPath()) + "> <" +
                            propertyHasSets + "> <" + converter.toDomain(setObject.getPath()) + ">}");
            setRoot.updateProperties(converter, sparql.toString(), new RdfStream());

            sparql.setLength(0);
            sparql.append("INSERT DATA {")
                    .append("<" + converter.toDomain(setObject.getPath()) + "> <" + propertySetName +
                            "> '" + set.getSetName() + "' .")
                    .append("<" + converter.toDomain(setObject.getPath()) + "> <" + propertyHasSetSpec +
                            "> '" + set.getSetName() + "' .");
            for (DescriptionType desc : set.getSetDescription()) {
                // TODO: save description
            }
            sparql.append("}");
            setObject.updateProperties(converter, sparql.toString(), new RdfStream());
            session.save();
            return setObject.getPath();
        } catch (JAXBException e) {
            e.printStackTrace();
            throw new RepositoryException(e);
        }
    }

    private String getSetId(SetType set) throws RepositoryException {
        if (set.getSetSpec() == null) {
            throw new RepositoryException("SetSpec can not be empty");
        }
        String id = set.getSetSpec();
        int colonPos = id.indexOf(':');
        while (colonPos > 0) {
            id = id.substring(colonPos + 1);
        }
        return id;
    }

    /**
     * List records.
     *
     * @param session the session
     * @param uriInfo the uri info
     * @param metadataPrefix the metadata prefix
     * @param from the from
     * @param until the until
     * @param set the set
     * @param offset the offset
     * @return the jAXB element
     * @throws RepositoryException the repository exception
     */
    public JAXBElement<OAIPMHtype> listRecords(Session session, UriInfo uriInfo, String metadataPrefix, String from,
                                               String until, String set, int offset) throws RepositoryException {

        final HttpResourceConverter converter =
                new HttpResourceConverter(session, uriInfo.getBaseUriBuilder().clone().path(FedoraNodes.class));

        if (metadataPrefix == null) {
            return error(VerbType.LIST_RECORDS, null, null, OAIPMHerrorcodeType.BAD_ARGUMENT,
                    "metadataprefix is invalid");
        }
        final MetadataFormat mdf = metadataFormats.get(metadataPrefix);
        if (mdf == null) {
            return error(VerbType.LIST_RECORDS, null, metadataPrefix,
                    OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT, "Unavailable metadata format");
        }
        DateTime fromDateTime = null;
        DateTime untilDateTime = null;
        try {
            fromDateTime = (from != null && !from.isEmpty()) ? dateFormat.parseDateTime(from) : null;
            untilDateTime = (until != null && !until.isEmpty()) ? dateFormat.parseDateTime(until) : null;
        } catch (IllegalArgumentException e) {
            return error(VerbType.LIST_RECORDS, null, metadataPrefix, OAIPMHerrorcodeType.BAD_ARGUMENT,
                    e.getMessage());
        }

        final StringBuilder sparql =
                new StringBuilder("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ")
                        .append("SELECT ?sub ?obj WHERE {?sub <" + RdfLexicon.HAS_MIXIN_TYPE +
                                "> \"fedora:object\" . ");

        final List<String> filters = new ArrayList<>();

        if (fromDateTime != null || untilDateTime != null) {
            sparql.append("?sub <").append(RdfLexicon.LAST_MODIFIED_DATE).append("> ?date . ");
            if (fromDateTime != null) {
                filters.add("?date >='" + from + "'^^xsd:dateTime ");
            }
            if (untilDateTime != null) {
                filters.add("?date <='" + until + "'^^xsd:dateTime ");
            }
        }

        if (set != null && !set.isEmpty()) {
            if (!setsEnabled) {
                return error(VerbType.LIST_RECORDS, null, metadataPrefix, OAIPMHerrorcodeType.NO_SET_HIERARCHY,
                        "Sets are not enabled");
            }
            sparql.append("?sub <").append(propertyIsPartOfSet).append("> ?set . ");
            filters.add("?set = '" + set + "'");
        }

        int filterCount = 0;
        for (String filter : filters) {
            if (filterCount++ == 0) {
                sparql.append("FILTER (");
            }
            sparql.append(filter).append(filterCount == filters.size() ? ")" : " && ");
        }
        sparql.append("}")
                .append(" OFFSET ").append(offset)
                .append(" LIMIT ").append(maxListSize);
        try {
            final JQLConverter jql = new JQLConverter(session, converter, sparql.toString());
            final ResultSet result = jql.execute();
            final OAIPMHtype oai = oaiFactory.createOAIPMHtype();
            final ListRecordsType records = oaiFactory.createListRecordsType();
            if (!result.hasNext()) {
                return error(VerbType.LIST_RECORDS, null, metadataPrefix, OAIPMHerrorcodeType.NO_RECORDS_MATCH,
                        "No record found");
            }
            while (result.hasNext()) {
                // check if the records exists
                final RecordType record = oaiFactory.createRecordType();
                final HeaderType h = oaiFactory.createHeaderType();
                final QuerySolution solution = result.next();
                final Resource subjectUri = solution.get("sub").asResource();
                h.setIdentifier(subjectUri.getURI());

                final FedoraObject obj =
                        this.objectService.findOrCreateObject(session, converter.asString(subjectUri));
                h.setDatestamp(dateFormat.print(obj.getLastModifiedDate().getTime()));
                // get set names this object is part of

                RdfStream triples = obj.getTriples(converter, PropertiesRdfContext.class).filter(
                        new PropertyPredicate(propertyIsPartOfSet));
                final List<String> setNames = new ArrayList<>();
                while (triples.hasNext()) {
                    setNames.add(triples.next().getObject().getLiteralValue().toString());
                }
                for (String name : setNames) {
                    final FedoraObject setObject = this.objectService.findOrCreateObject(session,
                            setsRootPath + "/" + name);
                    final RdfStream setTriples = setObject.getTriples(converter, PropertiesRdfContext.class).filter(
                            new PropertyPredicate(propertyHasSetSpec));
                    h.getSetSpec().add(setTriples.next().getObject().getLiteralValue().toString());
                }

                // get the metadata record from fcrepo
                final MetadataType md = this.oaiFactory.createMetadataType();
                if (metadataPrefix.equals("oai_dc")) {
                    /* generate a OAI DC reponse using the DC Generator from fcrepo4 */
                    md.setAny(generateOaiDc(session, obj.getNode().getIdentifier(), uriInfo));
                } else {
                    /* generate a OAI response from the linked Binary */
                    md.setAny(fetchOaiResponse(session, obj.getNode().getIdentifier(), mdf));
                }

                record.setMetadata(md);
                record.setHeader(h);
                records.getRecord().add(record);
            }

            final RequestType req = oaiFactory.createRequestType();
            if (records.getRecord().size() == maxListSize) {
                req.setResumptionToken(encodeResumptionToken(VerbType.LIST_RECORDS.value(), metadataPrefix, from,
                        until, set,
                        offset + maxListSize));
            }
            req.setVerb(VerbType.LIST_RECORDS);
            req.setMetadataPrefix(metadataPrefix);
            oai.setRequest(req);
            oai.setListRecords(records);
            return oaiFactory.createOAIPMH(oai);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RepositoryException(e);
        }
    }
}

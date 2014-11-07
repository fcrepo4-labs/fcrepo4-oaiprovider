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
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.fcrepo.http.api.FedoraLdp;
import org.fcrepo.http.api.FedoraNodes;
import org.fcrepo.http.commons.api.rdf.HttpResourceConverter;
import org.fcrepo.http.commons.session.SessionFactory;
import org.fcrepo.kernel.RdfLexicon;
import org.fcrepo.kernel.impl.rdf.impl.PropertiesRdfContext;
import org.fcrepo.kernel.models.Container;
import org.fcrepo.kernel.models.FedoraBinary;
import org.fcrepo.kernel.models.FedoraResource;
import org.fcrepo.kernel.services.BinaryService;
import org.fcrepo.kernel.services.ContainerService;
import org.fcrepo.kernel.services.NodeService;
import org.fcrepo.kernel.services.RepositoryService;
import org.fcrepo.kernel.utils.iterators.RdfStream;
import org.fcrepo.oai.dublincore.JcrPropertiesGenerator;
import org.fcrepo.oai.jql.JQLConverter;
import org.fcrepo.oai.rdf.PropertyPredicate;
import org.fcrepo.oai.http.ResumptionToken;
import org.fcrepo.oai.jersey.XmlDeclarationStrippingInputStream;
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
import org.openarchives.oai._2_0.oai_dc.OaiDcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * The type OAI provider service.
 *
 * @author Frank Asseg
 */
public class OAIProviderService {

    private static final Logger log = LoggerFactory.getLogger(OAIProviderService.class);

    private static final ObjectFactory oaiFactory = new ObjectFactory();

    private final DatatypeFactory dataFactory;

    private final Unmarshaller unmarshaller;

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
    private ContainerService containerService;

    @Autowired
    private RepositoryService repositoryService;

    @Autowired
    private JcrPropertiesGenerator jcrPropertiesGenerator;

    private final String OAI_REPONAME = "oai:repositoryName";

    private final String OAI_REPODESC = "oai:description";

    private final String OAI_REPOVERSION = "oai:version";

    private final String OAI_ADMINEMAIL = "oai:adminEmail";

    private final String OAI_STATICNAME = "Fedora 4";

    private final String OAI_STATICDESC = "An example repository description";

    private final String OAI_STATICVERSION = "x.y.z";

    private final String OAI_STATICEMAIL = "admin@example.com";

    /**
     * Sets property has set spec.
     *
     * @param propertyHasSetSpec the property has set spec
     */
    public void setPropertyHasSetSpec(final String propertyHasSetSpec) {
        this.propertyHasSetSpec = propertyHasSetSpec;
    }

    /**
     * Sets property set name.
     *
     * @param propertySetName the property set name
     */
    public void setPropertySetName(final String propertySetName) {
        this.propertySetName = propertySetName;
    }

    /**
     * Sets property has sets.
     *
     * @param propertyHasSets the property has sets
     */
    public void setPropertyHasSets(final String propertyHasSets) {
        this.propertyHasSets = propertyHasSets;
    }

    /**
     * Sets max list size.
     *
     * @param maxListSize the max list size
     */
    public void setMaxListSize(final int maxListSize) {
        this.maxListSize = maxListSize;
    }

    /**
     * Sets property is part of set.
     *
     * @param propertyIsPartOfSet the property is part of set
     */
    public void setPropertyIsPartOfSet(final String propertyIsPartOfSet) {
        this.propertyIsPartOfSet = propertyIsPartOfSet;
    }

    /**
     * Sets sets root path.
     *
     * @param setsRootPath the sets root path
     */
    public void setSetsRootPath(final String setsRootPath) {
        this.setsRootPath = setsRootPath;
    }

    /**
     * Sets sets enabled.
     *
     * @param setsEnabled the sets enabled
     */
    public void setSetsEnabled(final boolean setsEnabled) {
        this.setsEnabled = setsEnabled;
    }

    /**
     * Sets auto generate oai dc.
     *
     * @param autoGenerateOaiDc the auto generate oai dc
     */
    public void setAutoGenerateOaiDc(final boolean autoGenerateOaiDc) {
        this.autoGenerateOaiDc = autoGenerateOaiDc;
    }

    /**
     * Sets metadata formats.
     *
     * @param metadataFormats the metadata formats
     */
    public void setMetadataFormats(final Map<String, MetadataFormat> metadataFormats) {
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
        final Session session = sessionFactory.getInternalSession();
        if (!this.nodeService.exists(session, setsRootPath)) {
            this.containerService.findOrCreate(session, setsRootPath);
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
    public JAXBElement<OAIPMHtype> identify(final Session session, final UriInfo uriInfo) throws RepositoryException,
            JAXBException {

        final FedoraResource root = this.nodeService.find(session, "/");

        final IdentifyType id = this.oaiFactory.createIdentifyType();
        // TODO: Need real values here from the root node?
        id.setBaseURL(uriInfo.getBaseUri().toASCIIString());

        final String repoName;
        final String repoDescription;
        final String repoVersion;
        final String repoAdminEmail;
        if (root.hasProperty(OAI_ADMINEMAIL)) {
            repoAdminEmail = root.getProperty(OAI_ADMINEMAIL).getValues()[0].getString();
        } else {
            repoAdminEmail = OAI_STATICEMAIL;
        }
        if (root.hasProperty(OAI_REPONAME)) {
            repoName = root.getProperty(OAI_REPONAME).getValues()[0].getString();
        } else {
            repoName = OAI_STATICNAME;
        }

        final ClassPathResource resource = new ClassPathResource("app.properties");
        InputStream inputstream = null;
        String verstring = OAI_STATICVERSION;
        try {
            inputstream = resource.getInputStream();
            final Properties p = new Properties();
            p.load(inputstream);
            verstring = p.getProperty("application.version");
        } catch (IOException e) {
        }
        repoVersion = verstring;

        if (root.hasProperty(OAI_REPODESC)) {
            repoDescription = root.getProperty(OAI_REPODESC).getValues()[0].getString() +
                              " [" + repoVersion + "]";
        } else {
            repoDescription = OAI_STATICDESC + " [" + repoVersion + "]";
        }
        id.setEarliestDatestamp("INSTALL_DATE");
        id.setProtocolVersion("2.0");
        id.setRepositoryName(repoName);
        id.getAdminEmail().add(0,repoAdminEmail);
        final DescriptionType desc = this.oaiFactory.createDescriptionType();
        desc.setAny(new JAXBElement<String>(new QName("general"), String.class, repoDescription));
        id.getDescription().add(0, desc);

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
        final HttpResourceConverter converter = new HttpResourceConverter(session, uriInfo.getBaseUriBuilder()
                .clone().path(FedoraNodes.class));

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
                final Container obj = this.containerService.findOrCreate(session, "/" + identifier);
                for (MetadataFormat mdf : metadataFormats.values()) {
                    if (mdf.getPrefix().equals("oai_dc")) {
                        listMetadataFormats.getMetadataFormat().add(mdf.asMetadataFormatType());
                    } else {
                        final RdfStream triples = obj.getTriples(converter, PropertiesRdfContext.class).filter(
                                new PropertyPredicate(mdf.getPropertyName()));
                        if (triples.hasNext()) {
                            listMetadataFormats.getMetadataFormat().add(mdf.asMetadataFormatType());
                        }
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

        /* Prepare the OAI response objects */
        final Container obj = this.containerService.findOrCreate(session, "/" + identifier);

        final OAIPMHtype oai = oaiFactory.createOAIPMHtype();
        final RequestType req = oaiFactory.createRequestType();
        req.setVerb(VerbType.GET_RECORD);
        req.setValue(uriInfo.getRequestUri().toASCIIString());
        req.setMetadataPrefix("oai_dc");
        oai.setRequest(req);

        final GetRecordType getRecord = oaiFactory.createGetRecordType();
        final RecordType record;
        try {
            record = this.createRecord(session, format, obj.getPath(), uriInfo);
            getRecord.setRecord(record);
            oai.setGetRecord(getRecord);
            return this.oaiFactory.createOAIPMH(oai);
        } catch (IOException e) {
            log.error("Unable to create OAI record for object " + obj.getPath());
            return error(VerbType.GET_RECORD, identifier, metadataPrefix, OAIPMHerrorcodeType.ID_DOES_NOT_EXIST,
                    "The requested OAI record does not exist for object " + obj.getPath());
        }
    }

    private JAXBElement<OaiDcType> generateOaiDc(final Session session, final Container obj,
            final UriInfo uriInfo) throws RepositoryException {

        return jcrPropertiesGenerator.generateDC(session, obj, uriInfo);
    }

    private JAXBElement<String> fetchOaiResponse(final Container obj, final Session session,
            final MetadataFormat format, final UriInfo uriInfo) throws RepositoryException, IOException {

        final HttpResourceConverter converter = new HttpResourceConverter(session, uriInfo.getBaseUriBuilder().clone()
                .path(FedoraNodes.class));
        final RdfStream triples = obj.getTriples(converter, PropertiesRdfContext.class).filter(
                new PropertyPredicate(format.getPropertyName()));

        if (!triples.hasNext()) {
            log.error("There is no OAI record of type " + format.getPrefix() + " associated with the object "
                    + obj.getPath());
            return null;
        }

        final String recordPath = triples.next().getObject().getLiteralValue().toString();
        final FedoraBinary bin = binaryService.findOrCreate(session, "/" + recordPath);

        try (final InputStream src = new XmlDeclarationStrippingInputStream(bin.getContent())) {
            return new JAXBElement<String>(new QName(format.getPrefix()), String.class, IOUtils.toString(src));
        }
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
    public static JAXBElement<OAIPMHtype> error(final VerbType verb, final String identifier,
            final String metadataPrefix, final OAIPMHerrorcodeType errorCode, final String msg) {
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
    public JAXBElement<OAIPMHtype> listIdentifiers(final Session session, final UriInfo uriInfo,
                                                   final String metadataPrefix, final String from, final String until,
                                                   final String set, final int offset)
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
                                RdfLexicon.HAS_MIXIN_TYPE + "> \"fedora:Container\" . ");

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
                final Container obj =
                        this.containerService.findOrCreate(session, path);
                h.setDatestamp(dateFormat.print(obj.getLastModifiedDate().getTime()));


                final RdfStream triples = obj.getTriples(converter, PropertiesRdfContext.class).filter(
                        new PropertyPredicate(propertyIsPartOfSet));
                final List<String> setNames = new ArrayList<>();
                while (triples.hasNext()) {
                    setNames.add(triples.next().getObject().getLiteralValue().toString());
                }
                for (String name : setNames) {
                    final Container setObject = this.containerService.findOrCreate(session, setsRootPath + "/"
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
    public static String encodeResumptionToken(final String verb, final String metadataPrefix, final String from,
            final String until, final String set, final int offset) throws UnsupportedEncodingException {

        final String[] data = new String[] {
            urlEncode(verb),
            urlEncode(metadataPrefix),
            urlEncode(from != null ? from : ""),
            urlEncode(until != null ? until : ""),
            urlEncode(set != null ? set : ""),
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
    public static String urlEncode(final String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, "UTF-8");
    }

    /**
     * Url decode.
     *
     * @param value the value
     * @return the string
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    public static String urlDecode(final String value) throws UnsupportedEncodingException {
        return URLDecoder.decode(value, "UTF-8");
    }

    /**
     * Decode resumption token.
     *
     * @param token the token
     * @return the resumption token
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    public static ResumptionToken decodeResumptionToken(final String token) throws UnsupportedEncodingException {
        final String[] data = StringUtils.splitPreserveAllTokens(new String(Base64.decodeBase64(token)), ':');
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
    public JAXBElement<OAIPMHtype> listSets(final Session session, final UriInfo uriInfo, final int offset)
            throws RepositoryException {
        final HttpResourceConverter converter =
                new HttpResourceConverter(session, uriInfo.getBaseUriBuilder().clone().path(FedoraLdp.class));
        try {
            if (!setsEnabled) {
                return error(VerbType.LIST_SETS, null, null, OAIPMHerrorcodeType.NO_SET_HIERARCHY,
                        "Set are not enabled");
            }
            final StringBuilder sparql = new StringBuilder("SELECT ?obj WHERE {")
                    .append("<").append(converter.toDomain(setsRootPath)).append("> ")
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
                    final QuerySolution sol = setResult.next();
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
    public String createSet(final Session session, final UriInfo uriInfo, final InputStream src)
            throws RepositoryException {
        final HttpResourceConverter converter =
                new HttpResourceConverter(session, uriInfo.getBaseUriBuilder().clone().path(FedoraLdp.class));
        try {
            final SetType set = this.unmarshaller.unmarshal(new StreamSource(src), SetType.class).getValue();
            final String setId = getSetId(set);
            if (!this.nodeService.exists(session, setsRootPath)) {
                throw new RepositoryException("The root set object does not exist");
            }
            final Container setRoot = this.containerService.findOrCreate(session, setsRootPath);
            if (set.getSetSpec() != null) {
                /* validate that the hierarchy of sets exists */
            }

            if (this.nodeService.exists(session, setsRootPath + "/" + setId)) {
                throw new RepositoryException("The OAI Set with the id already exists");
            }
            final Container setObject = this.containerService.findOrCreate(session, setsRootPath + "/" + setId);

            final StringBuilder sparql =
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

    private String getSetId(final SetType set) throws RepositoryException {
        if (set.getSetSpec() == null) {
            throw new RepositoryException("SetSpec can not be empty");
        }
        String id = set.getSetSpec();
        final int colonPos = id.indexOf(':');
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
    public JAXBElement<OAIPMHtype> listRecords(final Session session, final UriInfo uriInfo,
                                               final String metadataPrefix, final String from, final String until,
                                               final String set, final int offset) throws RepositoryException {

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
                                "> \"fedora:Container\" . ");

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
                final QuerySolution solution = result.next();
                final Resource subjectUri = solution.get("sub").asResource();
                final RecordType record = this.createRecord(session, mdf, converter.asString(subjectUri), uriInfo);
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

    private RecordType createRecord(final Session session, final MetadataFormat mdf, final String s,
                                    final UriInfo uriInfo) throws IOException, RepositoryException {

        final HttpResourceConverter converter = new HttpResourceConverter(session, uriInfo.getBaseUriBuilder().clone()
                .path(FedoraNodes.class));
        final HeaderType h = oaiFactory.createHeaderType();
        final String subjectUri = converter.toDomain(s).getURI();
        h.setIdentifier(subjectUri);

        final Container obj =
                this.containerService.findOrCreate(session, s);
        h.setDatestamp(dateFormat.print(obj.getLastModifiedDate().getTime()));
        // get set names this object is part of

        final RdfStream triples = obj.getTriples(converter, PropertiesRdfContext.class).filter(
                new PropertyPredicate(propertyIsPartOfSet));
        final List<String> setNames = new ArrayList<>();
        while (triples.hasNext()) {
            setNames.add(triples.next().getObject().getLiteralValue().toString());
        }
        for (String name : setNames) {
            final Container setObject = this.containerService.findOrCreate(session,
                                                                           setsRootPath + "/" + name);
            final RdfStream setTriples = setObject.getTriples(converter, PropertiesRdfContext.class).filter(
                    new PropertyPredicate(propertyHasSetSpec));
            h.getSetSpec().add(setTriples.next().getObject().getLiteralValue().toString());
        }

        // get the metadata record from fcrepo
        final MetadataType md = this.oaiFactory.createMetadataType();
        if (mdf.getPrefix().equals("oai_dc")) {
            /* generate a OAI DC reponse using the DC Generator from fcrepo4 */
            md.setAny(generateOaiDc(session, obj, uriInfo));
        } else {
            /* generate a OAI response from the linked Binary */
            md.setAny(fetchOaiResponse(obj, session, mdf, uriInfo));
        }

        final RecordType record = this.oaiFactory.createRecordType();
        record.setMetadata(md);
        record.setHeader(h);
        return record;
    }
}

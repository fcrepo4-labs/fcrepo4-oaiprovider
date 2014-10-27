[![Build Status](https://travis-ci.org/fcrepo4-labs/fcrepo4-oaiprovider.svg?branch=master)](https://travis-ci.org/fcrepo4-labs/fcrepo4-oaiprovider)

OAI Provider for Fedora 4
=========================

Implements [Open Archives Protocol Version 2.0](http://www.openarchives.org/OAI/openarchivesprotocol.html) using Fedora 4 as the backend.

Implementation details
-------------
The OAI Provider exposes an endpoint at `/oai` which accepts OAI conforming HTTP requests.
A Fedora object containing the set information is created at `/oai/setspec`.
For Set creation an endpoint at `/oai/sets` is exposed which accepts HTTP POST requests containing serialized Set information adhering to the OAI schema.

The provider depends on the [fcrepo-dc-generator](https://github.com/fcrepo4-labs/fcrepo-generator-dc) for creating a default oai_dc responses.

For advanced use-cases the provider depends on links to Datastreams and OAI Set objects to generate OAI responses.
A graph linking a Fedora Object to it's OAI DC Datastream should look like this:
 
                                                 +----------+
                                                 | MyObject | 
                                                 +----------+
                                                 /          \
                                                /            \
                                        hasOaiDCRecord   isPartOfOaiSet
                                              /                \
                                             /                  \
                                +-------------------+      +----------+
                                | MyOaiDCDatastream |      |   MySet  |
                                +-------------------+      +----------+
                                
Additional Metadata record types
--------------------------------

The oaiprovider supports `oai_dc` out if the box, but users are able to add their own metadata format definitions to oai.xml.

Examples
-------

Create a Fedora Object `MyObject` and add an OAI DC Datastream `MyOAIDCDatastream` which is contained in the OAI Set `MyOAISet` 

1.Create a new Datastream containing an XML representation of an oai_dc record:

```bash
#> curl -X POST http://localhost:8080/fcrepo/rest -H "Slug:MyOAIDCDatastream" -H "Content-Type:application/octet-stream" --data @src/test/resources/test-data/oaidc.xml
```

2.Create a new OAI Set:

```bash
#> curl -X POST http://localhost:8080/fcrepo/rest/oai/sets -H "Content-Type:text/xml" --data @src/test/resources/test-data/set.xml
```

3.Create a new Fedora Object and link the oai_dc Datastream and the OAI Set created in the previous step to it:

```bash
#> curl -X POST http://localhost:8080/fcrepo/rest -H "Slug:MyObject" -H "Content-Type:application/sparql-update"  --data "INSERT {<> <http://fedora.info/definitions/v4/config#hasOaiDCRecord> <http://localhost:8080/fcrepo/rest/MyOAIDCDatastream> . <> <http://fedora.info/definitions/v4/config#isPartOfOAISet> \"MyOAISet\"} WHERE {}"
```

4.Try the various responses from the oai provider

```bash
#> curl http://localhost:8080/fcrepo/rest/oai?verb=ListMetadataFormats
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=GetRecord&identifier=MyObject&metadataPrefix=oai_dc"
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=ListRecords&metadataPrefix=oai_dc"
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=ListIdentifiers&metadataPrefix=oai_dc"
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=ListSets"
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=ListIdentifiers&metadataPrefix=oai_dc&set=MyOAISet"
```
                                

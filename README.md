[![Build Status](https://travis-ci.org/fcrepo4-labs/fcrepo4-oaiprovider.svg?branch=master)](https://travis-ci.org/fcrepo4-labs/fcrepo4-oaiprovider)

OAI Provider for Fedora 4
=========================

Implements [Open Archives Protocol Version 2.0](http://www.openarchives.org/OAI/openarchivesprotocol.html) using Fedora 4 as the backend.

Implementation details
-------------
The OAI Provider exposes an endpoint at `http://localhost:8080/fcrepo/rest/oai` which accepts OAI conforming HTTP requests.
A Fedora object containing the set information is created at `/oai/setspec`.
For Set creation an endpoint at `/oai/sets` is exposed which accepts HTTP POST requests containing serialized Set information adhering to the OAI schema.

The provider depends on the [fcrepo-dc-generator](https://github.com/fcrepo4-labs/fcrepo-generator-dc) for creating a default oai_dc responses.

For advanced use-cases the provider depends on links to FedoraBinary instances and OAI Set objects to generate OAI responses.
A graph linking a Fedora Object to it's OAI metadata should look like this:
 
                                                 +----------+
                                                 | MyObject | 
                                                 +----------+
                                                 /          \
                                                /            \
                                     hasOaiMarc21Record   isPartOfOaiSet
                                              /                \
                                             /                  \
                                   +----------------+      +----------+
                                   | MyMarc21Binary |      |   MySet  |
                                   +----------------+      +----------+
                                
Additional Metadata record types
--------------------------------

The oaiprovider supports `oai_dc` out if the box, but users are able to add their own metadata format definitions to oai.xml.

Installation
------------
Currently installation involves copying files by hand to an exploded fcrepo4 web application

**The following dependencies have to be copied to Fedora 4's lib directory**
 - [JAX-B Implementation 2.2.7](http://mvnrepository.com/artifact/com.sun.xml.bind/jaxb-impl/2.2.7)
 - [JAX-B Core 2.2.7](http://mvnrepository.com/artifact/com.sun.xml.bind/jaxb-core/2.2.7)
 
```bash
#> wget -P /path/to/fcrepo/WEB-INF/lib http://central.maven.org/maven2/com/sun/xml/bind/jaxb-impl/2.2.7/jaxb-impl-2.2.7.jar
#> wget -P /path/to/fcrepo/WEB-INF/lib http://central.maven.org/maven2/com/sun/xml/bind/jaxb-core/2.2.7/jaxb-core-2.2.7.jar
```
 
**Build and copy the oai provider to the Fedora 4's lib directory**
```bash
#> git clone https://github.com/fcrepo4-labs/fcrepo4-oaiprovider.git
#> cd fcrepo4-oaiprovider
#> mvn package
#> cp target/fcrepo-oaiprovider-4.0.0-beta-04-SNAPSHOT.jar /path/to/fcrepo/WEB-INF/lib/
```

**Copy the oai.xml Spring configuration to Fedora 4's config directory**

```bash
#> cp fcrepo4-oaiprovider/src/main/resources/spring/oai.xml /path/to/fcrepo/WEB-INF/classes/spring/
```

**Add `<import resource="classpath:/spring/oai.xml"/>` to Fedora 4's master.xml configuration file**

```bash
#> vim /path/to/fcrepo/WEB-INF/classes/spring/master.xml
```

After restarting Fedora 4 the OAI Provider is available at http://localhost:8080/fcrepo/rest/oai


Example
-------

In order to get some results, a couple of objects can be created:

```bash
#> curl -X POST http://localhost:8080/fcrepo/rest -H "Slug:foo""
#> curl -X POST http://localhost:8080/fcrepo/rest -H "Slug:bar""
```

Try the various responses from the oai provider:

```bash
#> curl http://localhost:8080/fcrepo/rest/oai?verb=ListMetadataFormats
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=GetRecord&identifier=MyObject&metadataPrefix=oai_dc"
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=ListRecords&metadataPrefix=oai_dc"
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=ListIdentifiers&metadataPrefix=oai_dc"
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=ListSets"
#> curl "http://localhost:8080/fcrepo/rest/oai?verb=ListIdentifiers&metadataPrefix=oai_dc&set=MyOAISet"
```

More examples can be found in [Integration Tests](https://github.com/fcrepo4-labs/fcrepo4-oaiprovider/tree/master/src/test/java/org/fcrepo/oai/integration)
                                

from woqlclient import WOQLClient, WOQLQuery
import pandas as pd
import json

server_url = "http://localhost:6363"
key = "root"
dbId = "schema_dot_org"

SIMPLE_TYPE=["http://schema.org/Boolean",
             "http://schema.org/Text",
             "http://schema.org/Date",
             "http://schema.org/URL",
             "http://schema.org/Integer",
             "http://schema.org/Number"
             ]

def construct_objects(series):
    result = WOQLQuery().\
             doctype(series.id).\
             label(series.label).\
             description(series.comment)
    return result

def construct_type_addon(series, type_id, prop_id):
    result = []
    if type(series.subTypes) == str:
        for kid in series.subTypes.split(','):
            kid = kid.strip()
            if kid in list(type_id):
                result.append(WOQLQuery().add_quad(kid, "subClassOf", series.id, "schema"))
    if type(series.subTypeOf) == str:
        for parent in series.subTypeOf.split(','):
            parent = parent.strip()
            if parent in list(type_id):
                result.append(WOQLQuery().add_quad(series.id, "subClassOf", parent, "schema"))
    return result

def construct_property_addon(series, type_id):
    result = [WOQLQuery().add_quad(series.id, "rdf:type", "owl:ObjectProperty", "schema")]
    if type(series.domainIncludes) == str:
        for domain in series.domainIncludes.split(','):
            domain = domain.strip()
            if domain in list(type_id):
                result.append(WOQLQuery().add_quad(series.id, "domain", domain, "schema"))
    if type(series.rangeIncludes) == str:
        for range in series.rangeIncludes.split(','):
            range = range.strip()
            if range in list(type_id):
                result.append(WOQLQuery().add_quad(series.id, "range", range, "schema"))
            elif range in SIMPLE_TYPE:
                result[0] = WOQLQuery().add_quad(series.id, "rdf:type", "owl:DatatypeProperty", "schema")
                result.append(WOQLQuery().add_quad(series.id, "range", "xsd:anySimpleType", "schema"))
    if len(result) == 1:
        print(series)
        return []
    return result

def create_schema_from_queries(client, queries):
    schema = WOQLQuery().when(True).woql_and(*queries)
    return schema.execute(client)

def create_schema_from_addon(client, queries):
    new_queries = []
    for query in queries:
        if len(query) > 0:
            new_queries.append(WOQLQuery().woql_and(*query))
    schema = WOQLQuery().when(True).woql_and(*new_queries)
    return schema.execute(client)

print("read types from csv and construct WOQL objects")
types = pd.read_csv("all-layers-types.csv")
types["WOQLObject"] = types.apply(construct_objects, axis=1)
#types.index = types.id

print("read perperties from csv and construct WOQL objects")
properties = pd.read_csv("all-layers-properties.csv")
properties["WOQLObject"] = properties.apply(construct_objects, axis=1)
#properties.index = properties.id

print("create types addon")
types["addon"] = types.apply(construct_type_addon,
                             axis=1,
                             type_id=types.id,
                             prop_id=properties.id)

print("create properties addon")
properties["addon"] = properties.apply(construct_property_addon,
                                       axis=1,
                                       type_id=types.id)

print("create db and schema")
client = WOQLClient()
client.connect(server_url, key)
client.deleteDatabase(dbId)
client.createDatabase(dbId, "Schema.org")
create_schema_from_queries(client, list(types["WOQLObject"]))
create_schema_from_queries(client, list(properties["WOQLObject"]))
create_schema_from_addon(client, list(types["addon"]))
create_schema_from_addon(client, list(properties["addon"]))

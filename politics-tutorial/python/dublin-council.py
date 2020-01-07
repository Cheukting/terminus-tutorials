import woqlclient.woqlClient as woql
from woqlclient import WOQLQuery
import json

CSVS = {"OverallSimilarity" : "https://terminusdb.com/t/data/council/weighted_similarity (1).csv"}

server_url = "http://localhost:6363"
key = "root"
dburl = server_url + "/mydb"

def create_schema(client):
    """The query which creates the schema
        Parameters
        ==========
        client : a WOQLClient() connection
    """
    schema = WOQLQuery().when(True).woql_and(
        WOQLQuery().doctype("Party").label("Party").\
                    description("Political Party"),
        WOQLQuery().doctype("Representative").label("Representative").\
                    description("An elected member of the US congress").\
                    property("member_of", "Party").label("Member of").cardinality(1),
        WOQLQuery().doctype("Similarity").label("Similarity").\
                    property("similarity", "decimal").label("Similarity").\
                    property("similar_to", "Representative").label("Similar To").cardinality(2),
        WOQLQuery().add_class("ArmedForcesSimilarity").label("Armed Forces").parent("Similarity"),
        WOQLQuery().add_class("CivilRightsSimilarity").label("Civil Rights").parent("Similarity"),
        WOQLQuery().add_class("HealthSimilarity").label("Health").parent("Similarity"),
        WOQLQuery().add_class("ImmigrationSimilarity").label("Immigration").parent("Similarity"),
        WOQLQuery().add_class("InternationalAffairsSimilarity").\
                    label("International Affairs").parent("Similarity"),
        WOQLQuery().add_class("TaxationSimilarity").label("Taxation").parent("Similarity"),
        WOQLQuery().add_class("OverallSimilarity").label("Overall").parent("Similarity")
        )
    return schema.execute(client)

def get_inserts(relation):
    inserts = WOQLQuery().woql_and(
        WOQLQuery().insert("v:Party_A_ID", "Party").label("v:Party_A"),
        WOQLQuery().insert("v:Party_B_ID", "Party").label("v:Party_B"),
        WOQLQuery().insert("v:Rep_A_ID", "Representative").label("v:Rep_A").\
                    property("member_of", "v:Party_A_ID"),
        WOQLQuery().insert("v:Rep_B_ID", "Representative").label("v:Rep_B").\
                    property("member_of", "v:Party_B_ID"),
        WOQLQuery().insert("v:Rel_ID", "Similarity").label("v:Rel_Label").\
                    property("similar_to", "v:Rep_A_ID").\
                    property("similar_to", "v:Rep_B_ID").\
                    property("similarity", "v:Similarity")
      )
    return inserts

def get_csv_variables(url):
    """Extracting the data from a CSV and binding it to variables
       Parameters
       ==========
       client : a WOQLClient() connection
       url : string, the URL of the CSV
       """
    csv = WOQLQuery().get(
        WOQLQuery().woql_as("councillor_a","v:Rep_A").\
                    woql_as("councillor_b", "v:Rep_B").\
                    woql_as("party_a", "v:Party_A").\
                    woql_as("party_b", "v:Party_B").\
                    woql_as("distance", "v:Distance")
        ).remote(url)
    return csv

def get_wrangles(relation):
    wrangles = [
         WOQLQuery().idgen("doc:Party", ["v:Party_A"], "v:Party_A_ID"),
         WOQLQuery().idgen("doc:Party", ["v:Party_B"], "v:Party_B_ID"),
         WOQLQuery().idgen("doc:Representative", ["v:Rep_A"], "v:Rep_A_ID"),
         WOQLQuery().idgen("doc:Representative", ["v:Rep_B"], "v:Rep_B_ID"),
         WOQLQuery().typecast("v:Distance", "xsd:decimal", "v:Similarity"),
         WOQLQuery().idgen("doc:Similarity", ["v:Rep_A", "v:Rep_B"], "v:Rel_ID"),
         WOQLQuery().concat("v:Distance between v:Rep_A and v:Rep_B", "v:Rel_Label")
    ]
    return wrangles

def load_csvs(client, csvs):
    """Load the CSVs as input
       Parameters
       ==========
       client : a WOQLClient() connection
       csvs : a dict of all csvs to be input
    """
    for key, url in csvs.items:
        csv = get_csv_variables(url)
        wrangles = get_wrangles(key)
        inputs = WOQLQuery().woql_and(csv, *wrangles)
        inserts = getInserts(key)
        answer = WOQLQuery().when(inputs, inserts)
        answer.execute(client)

# run tutorial
client = woql.WOQLClient()
client.connect(server_url, key)
client.directCreateDatabase(dburl, "Politics Graph", key)
create_schema(client)
load_csvs(client, CSVS)

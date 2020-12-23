from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
password = "hello@123"
driver = GraphDatabase.driver(uri, auth=(user, password))


def recommendations(service = '', location = '', client = ''):
    client_list = []
    if client == '':
        client_list = []
    else:
        client_list = client.split(',')

    service_name = service
    location_name = location
    services = "(service)" if service_name == '' else "(service:provider_serv {name: $service_name})"
    locations = "(location)" if location_name == '' else "(location:provider_loc {name: $location_name})"
    clients = "" if len(client_list) == 0 else "WHERE client.name IN %s" % (str(client_list))
    neo4j_query = (
            '''MATCH (provider:provider_name)-[:Located_In]->%s,
                  (provider)-[:provides]->%s,
                  (client:client_Name)-[:Uses]->(provider)
            %s RETURN provider.name AS name, collect(client.name) AS who_takes_services, COUNT(*) AS counts
            ORDER BY counts DESC''' % (locations, services, clients))


    result = session.run(neo4j_query, service_name=service_name, location_name=location_name, client_list=client_list)

    return [{"provider": row["name"], "who_takes_services": row["who_takes_services"], "counts": row["counts"]} for row in result]



print(recommendations(client='Boston Locals'))

print(recommendations(service='Fiber'))

print(recommendations(location='APAC'))

import time
from neo4j import GraphDatabase

class PollyGraph:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response

def find_datasets_using_term(node_type,node_property,term):
    node_label = "ns0__"+node_type
    properties = ""
    for i in node_property:
        prop_label = "ns0__"+i
        properties = properties +"n."+prop_label+"+ "
    properties = properties[0:len(properties)-2]+" "
    query = f"""MATCH (p: dataset)--(n:{node_label}) WHERE 
    ANY (x in {properties}where toLower(x) CONTAINS("{term.lower()}"))
    RETURN p.src_dataset_id as src_dataset_id,n.ns0__name as term;"""
    return query

def find_datasets_using_related_terms(node_type,node_property,term):
    node_label = "ns0__"+node_type
    properties = ""
    for i in node_property:
        prop_label = "ns0__"+i
        properties = properties +"n."+prop_label+"+ "
    properties = properties[0:len(properties)-2]+" "
    relation = "ns0__is_a_"+node_type
    query = f"""MATCH (n:{node_label})-[:{relation}]-(m:{node_label}) 
    WHERE ANY (x in {properties}WHERE tolower(x) CONTAINS('{term.lower()}'))
    WITH {properties}as terms
    UNWIND terms as t 
    MATCH (p:dataset)--(n:{node_label} """+"{ns0__name: [t]}"+""") 
    RETURN p.src_dataset_id as dataset_id,n.ns0__name AS name;"""
    return query 

def build_rel_query(field_name, col_name, meta_field, rel):
    
    if rel == 'mentioned_in':
        q = """
            UNWIND $rows as row
            MATCH (n:ns0__"""+field_name +"""{ns0__"""+meta_field+""": [row."""+col_name+"""]})
            MATCH (d:dataset {src_dataset_id: row.src_dataset_id})
            MERGE (n)-[:"""+rel+"""]->(d)
            RETURN count(*) as total
            """
    else:
        q = """
            UNWIND $rows as row
            MATCH (n:ns0__"""+field_name +"""{ns0__"""+meta_field+""": row."""+col_name+"""})
            MATCH (d:dataset {src_dataset_id: row.src_dataset_id})
            MERGE (n)-[:"""+rel+"""]->(d)
            RETURN count(*) as total
            """
        
    return(q)

def insert_data(query, rows, batch_size = 500):
    # Function to handle the updating the Neo4j database in batch mode.

    total = 0
    batch = 0
    start = time.time()
    result = None

    while batch * batch_size < len(rows):
        res = pollygraph.query(query, 
                         parameters = {'rows': rows[batch*batch_size:(batch+1)*batch_size].to_dict('records')})
        total += res[0]['total']
        batch += 1
        result = {"total":total, 
                  "batches":batch, 
                  "time":time.time()-start}
        #print(result)

    return result

def add_dataset(rows, batch_size=500):
   # Adds category nodes to the Neo4j graph.
    query = '''
            UNWIND $rows AS row
            MERGE (c:dataset {
                dataset_id: row.dataset_id,
                src_dataset_id: row.src_dataset_id, 
                src_overall_design: row.src_overall_design,
                src_description: row.src_description,
                src_summary: row.src_summary,
                data_type: row.data_type,
                curated_cell_line: row.curated_cell_line,
                curated_cell_type: row.curated_cell_type,
                curated_disease: row.curated_disease,
                curated_drug: row.curated_drug,
                curated_gene: row.curated_gene,
                curated_tissue: row.curated_tissue,
                condition_column: row.condition_column,
                condition_control: row.condition_control,
                condition_perturbation: row.condition_perturbation
                })
            
            RETURN count(*) as total
            '''
    return insert_data(query, rows)
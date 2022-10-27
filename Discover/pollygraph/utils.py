import time
from neo4j import GraphDatabase
import pandas as pd

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

def find_datasets_using_term(node_type,term,node_property=['name','synonyms']):
    pollygraph = PollyGraph(uri="bolt://localhost:7687", user="neo4j", pwd="password")
    node_label = "ns0__"+node_type
    properties = ""
    dataset_return_properties = ['data_type', 'dataset_id', 'curated_disease', 'curated_drug', 'curated_gene', 'curated_tissue',
                                 'condition_control', 'condition_perturbation', 'curated_cell_line', 'curated_cell_type',
                                 'src_overall_design', 'src_summary', 'condition_column', 'src_dataset_id', 'src_description']
    for i in node_property:
        prop_label = "ns0__"+i
        properties = properties +"n."+prop_label+"+ "
    properties = properties[0:len(properties)-2]+" "
    ret = make_return_statement_dataset({'p':dataset_return_properties})
    query = f"""MATCH (p: dataset)-[r1]-(n:{node_label}) WHERE 
    ANY (x in {properties}where toLower(x) CONTAINS("{term.lower()}"))
    RETURN {ret},n.ns0__name as term;"""
    top_cat_df = pd.DataFrame([dict(_) for _ in pollygraph.query(query)])
    plot_bar('node1_src_dataset_id','term',top_cat_df,'Datasets per term','Terms','Datasets')
    return top_cat_df

def find_datasets_using_related_term(node_type,term,node_property=['name','synonyms']):
    pollygraph = PollyGraph(uri="bolt://localhost:7687", user="neo4j", pwd="password")
    node_label = "ns0__"+node_type
    properties = ""
    dataset_return_properties = ['data_type', 'dataset_id', 'curated_disease', 'curated_drug', 'curated_gene', 'curated_tissue',
                                 'condition_control', 'condition_perturbation', 'curated_cell_line', 'curated_cell_type',
                                 'src_overall_design', 'src_summary', 'condition_column', 'src_dataset_id', 'src_description']
    for i in node_property:
        prop_label = "ns0__"+i
        properties = properties +"n."+prop_label+"+ "
    properties = properties[0:len(properties)-2]+" "
    #relation = "ns0__is_a_"+node_type
    ret = make_return_statement_dataset({'p':dataset_return_properties})
    query = f"""MATCH (n:{node_label})--(m:{node_label}) 
    WHERE ANY (x in {properties}WHERE tolower(x) CONTAINS('{term.lower()}'))
    WITH {properties}as terms
    UNWIND terms as t 
    MATCH (p:dataset)--(n:{node_label} """+"{ns0__name: [t]}"+f""") 
    RETURN {ret},n.ns0__name AS term;"""
    top_cat_df = pd.DataFrame([dict(_) for _ in pollygraph.query(query)])
    plot_bar('node1_src_dataset_id','term',top_cat_df,'Datasets per term','Terms','Datasets')
    return top_cat_df

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

def get_relation(node1,node2):
    pollygraph = PollyGraph(uri="bolt://localhost:7687", user="neo4j", pwd="password")
    query = f"""MATCH (n:ns0__{node1})-[r]-(m:ns0__{node2})
    RETURN distinct type(r) as relation_type;"""
    top_cat_df = pd.DataFrame([dict(_) for _ in pollygraph.query(query)])
    return top_cat_df

def get_related_nodes(node1,node2,node1_search_term,node2_search_term=None,relation=None,node1_search_properties=['name','synonyms'],node2_search_properties=['name','synonyms'],node1_return=['name'],node2_return=['name']):
    if node1 != 'dataset':
        node1= f'ns0__{node1}'
    else:
        node1=node1
    if node2 != 'dataset':
        node2 = f'ns0__{node2}'
    else:
        node2=node2
    properties_node1 = ""
    for i in node1_search_properties:
        prop_label = "ns0__"+i
        properties_node1 = properties_node1 +"n."+prop_label+"+ "
    properties_node1 = properties_node1[0:len(properties_node1)-2]+" "
    
    properties_node2 = ""
    for i in node2_search_properties:
        prop_label = "ns0__"+i
        properties_node2 = properties_node2 +"m."+prop_label+"+ "
    properties_node2 = properties_node2[0:len(properties_node2)-2]+" "
    ret = make_return_statement({'n':node1_return,'m':node2_return})
    if relation:
        if node2_search_term:
            query = f"""MATCH (n:{node1})-[r1:ns0__{relation}]-(m:{node2})
            WHERE ANY (x in {properties_node1}WHERE toLower(x) CONTAINS("{node1_search_term.lower()}")) AND
            ANY (y in {properties_node2}WHERE toLower(y) CONTAINS("{node2_search_term.lower()}"))
            RETURN {ret},type(r1) as relation;"""
        else:
            query = f"""MATCH (n:{node1})-[r1:ns0__{relation}]-(m:{node2})
            WHERE ANY (x in {properties_node1}WHERE toLower(x) CONTAINS("{node1_search_term.lower()}"))
            RETURN {ret},type(r1) as relation;"""
    else:
        if node2_search_term:
            query = f"""MATCH (n:{node1})-[r1]-(m:{node2})
            WHERE ANY (x in {properties_node1}WHERE toLower(x) CONTAINS("{node1_search_term.lower()}")) AND
            ANY (y in {properties_node2}WHERE toLower(y) CONTAINS("{node2_search_term.lower()}"))
            RETURN {ret},type(r1) as relation;"""
        else:
            query = f"""MATCH (n:{node1})-[r1]-(m:{node2})
            WHERE ANY (x in {properties_node1}WHERE toLower(x) CONTAINS("{node1_search_term.lower()}"))
            RETURN {ret},type(r1) as relation;"""
    return query

def get_node_properties(node):
    pollygraph = PollyGraph(uri="bolt://localhost:7687", user="neo4j", pwd="password")
    if node != 'dataset':
        node = f'ns0__{node}'
    else:
        node=node
    query = f"""MATCH (n:{node})
    UNWIND keys(n) as property
    RETURN DISTINCT(property);"""
    top_cat_df = pd.DataFrame([dict(_) for _ in pollygraph.query(query)])
    return top_cat_df

def make_return_statement(dict_return):
    return_query = ""
    j=0
    for i in dict_return.keys():
        j+=1
        node_name = 'node'+str(j)
        for k in dict_return[i]:
            return_query = f"{return_query}{i}.ns0__{k} as {node_name}_{k}, "
    return_query = return_query[0:len(return_query)-2]
    return return_query

def make_return_statement_dataset(dict_return):
    return_query = ""
    j=0
    for i in dict_return.keys():
        j+=1
        node_name = 'node'+str(j)
        for k in dict_return[i]:
            return_query = f"{return_query}{i}.{k} as {node_name}_{k}, "
    return_query = return_query[0:len(return_query)-2]
    return return_query

def plot_bar(id_col,term_col,data,title,x_label,y_label):
    import seaborn as sns
    import matplotlib.pyplot as plt
    df = data[[id_col,term_col]]
    for i in df.columns:
        df=df.explode(i)
    df = df.drop_duplicates(keep='first')
    print(f'No. of {y_label}:',len(df[id_col].unique()))
    print(f'No. of {x_label}:',len(df[term_col].unique()))
    sns.set_style('darkgrid')
    sns.set_palette('Set2')
    ax = sns.countplot(data=df, x=term_col)
    plt.title(title)
    plt.xlabel(x_label)
    plt.xticks(rotation=90)
    plt.ylabel(y_label)
    sns.despine()
    plt.show()

# Bryans individual is not limited to BryanIndividual.ipynb
from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import networkx as nx
from UtilsNeo4J import setup_neo4j_driver, get_total_unique_entries

driver = setup_neo4j_driver(
    uri="neo4j+s://abc.databases.neo4j.io",
    user="neo4j",
    password="abc"  # Remember to replace with your actual password!
)

# Get the total number of unique entries in the lexicon
total_unique_entries = get_total_unique_entries(driver)
print(f"Total number of unique entries: {total_unique_entries}")


def fetch_synonyms(tx, limit=25):
    query = f"""
    MATCH (w:Word)-[:SYNONYM]-(s:Word)
    RETURN w.word AS word, collect(DISTINCT s.word) AS synonyms
    LIMIT {limit}
    """
    result = tx.run(query)
    return result.values()

def create_synonym_network(driver, limit=25):
    with driver.session() as session:
        synonyms = session.read_transaction(fetch_synonyms, limit)
    
    G = nx.Graph()
    for word, syns in synonyms:
        for syn in syns:
            G.add_edge(word, syn)
    
    return G

def visualize_network(G):
    pos = nx.spring_layout(G, k=0.55)
    plt.figure(figsize=(20, 10))
    nx.draw(G, pos, with_labels=True, node_color='lightblue', edge_color='gray', node_size=5000, font_size=13)
    plt.title("Synonym Network")
    plt.show()

def identify_clusters(G):
    clusters = nx.community.greedy_modularity_communities(G)
    themes = {i: list(cluster) for i, cluster in enumerate(clusters)}
    return themes


G = create_synonym_network(driver, limit=30)  # Limit to 25 words
visualize_network(G)

themes = identify_clusters(G)
for theme_id, words in themes.items():
    print(f"Theme {theme_id}: {', '.join(words)}")



def get_synonyms(driver, word):
    """
    Retrieve synonyms for a given word.

    Args:
        driver (neo4j.Driver): The Neo4j driver instance.
        word (str): The word to search for.

    Returns:
        list: A list of synonyms.
    """
    query = """
    MATCH (w:Word {word: $word})-[:SYNONYM]->(synonym:Word)
    RETURN synonym.word AS synonym
    """
    with driver.session() as session:
        result = session.run(query, word=word)
        return [record["synonym"] for record in result]

def get_antonyms(driver, word):
    """
    Retrieve antonyms for a given word.

    Args:
        driver (neo4j.Driver): The Neo4j driver instance.
        word (str): The word to search for.

    Returns:
        list: A list of antonyms.
    """
    query = """
    MATCH (w:Word {word: $word})-[:ANTONYM]->(antonym:Word)
    RETURN antonym.word AS antonym
    """
    with driver.session() as session:
        result = session.run(query, word=word)
        return [record["antonym"] for record in result]


word_to_search = "gembira"
synonyms = get_synonyms(driver, word_to_search)
antonyms = get_antonyms(driver, word_to_search)

print(f"Synonyms for '{word_to_search}': {', '.join(synonyms)}")
print(f"Antonyms for '{word_to_search}': {', '.join(antonyms)}")
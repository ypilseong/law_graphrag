from langchain.chains import GraphCypherQAChain
from langchain_community.graphs import Neo4jGraph
from langchain_openai import ChatOpenAI
import os

graph = Neo4jGraph(url="bolt://", username="neo4j", password="") # url: bolt:// ~ , sandbox에서 확인한 log in info

chain = GraphCypherQAChain.from_llm(
    ChatOpenAI(temperature=0), graph=graph, verbose=True
)

result = chain.run("2014가합3266 판결문에 대해 알려줘")


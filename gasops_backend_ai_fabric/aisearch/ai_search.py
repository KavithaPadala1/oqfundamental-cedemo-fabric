import logging
from langchain_community.vectorstores.azuresearch import AzureSearch
from langchain_openai import AzureOpenAIEmbeddings
from langchain_text_splitters import CharacterTextSplitter
from langchain_community.document_loaders import TextLoader
from langchain_core.documents import Document

import os
import sys


# Configure logging
# logging.basicConfig(level=logging.DEBUG)

from dotenv import load_dotenv
load_dotenv()   

# Azure configuration
azure_openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
azure_openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
azure_openai_api_version = os.getenv("AZURE_OPENAI_API_VERSION")

azure_embedding_deployment = os.getenv("AZURE_SEARCH_DEPLOYMENT")   # Embedding deployment name
vector_store_address = os.getenv("AZURE_SEARCH_ENDPOINT")
vector_store_password = os.getenv("AZURE_SEARCH_KEY")

index_name = "oqfundamentalexamples_fabric"


# Embeddings: use Azure OpenAI endpoint/key/deployment for embeddings
embeddings = AzureOpenAIEmbeddings(
    azure_deployment=azure_embedding_deployment,
    openai_api_version=azure_openai_api_version,
    azure_endpoint=azure_openai_endpoint,
    api_key=azure_openai_api_key,
)


def oqfundamental_search(user_text: str):
    index_name = "oqfundamentalexamples_fabric"

    vector_store = AzureSearch(
        azure_search_endpoint=vector_store_address,
        azure_search_key=vector_store_password,
        index_name=index_name,
        embedding_function=embeddings.embed_query,
        additional_search_client_options={"retry_total": 4},
    )

    results = vector_store.similarity_search(query=user_text, k=2, search_type="similarity")
    return results

# testing ai search results
if __name__ == "__main__":
        user_question = "show me workorders for G23901"
        results = oqfundamental_search(user_question)
        print("AI Search Results for question:", user_question)
        for i, res in enumerate(results, 1):
            print(f"Result {i}:")
            print(res)



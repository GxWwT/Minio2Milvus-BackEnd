import os
import torch
import logging
from pymilvus import utility, connections, Collection
from langchain.vectorstores import Milvus
from langchain.embeddings import HuggingFaceBgeEmbeddings
from langchain.text_splitter import Language, RecursiveCharacterTextSplitter


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__)

DOCUMENT_MAP = {
    ".txt": "TextLoader",
    ".py": "TextLoader",
    ".json": "TextLoader",
    ".md": "UnstructuredMarkdownLoader",
    ".pdf": "PDFMinerLoader",
    ".csv": "CSVLoader",
    ".xls": "UnstructuredExcelLoader",
    ".xlsx": "UnstructuredExcelLoader",
    ".docx": "Docx2txtLoader", # pip install docx2txt -i https://pypi.tuna.tsinghua.edu.cn/simple
    ".doc": "Docx2txtLoader",
    ".html": "UnstructuredHTMLLoader",
    ".pptx": "UnstructuredPowerPointLoader",
}

def preprocess(file_path: str, source: str):
    # Loads a single document from a file path
    # source: "selected_bucket/object"
    file_extension = os.path.splitext(file_path)[1]
    loader_class = DOCUMENT_MAP.get(file_extension)
    if loader_class:
        exec("from " + "langchain.document_loaders" + " import " + loader_class)
        loader = locals().get(loader_class)
        loader = loader(file_path)
        raw_documents = loader.load()
        for doc in raw_documents:
            doc.metadata['source'] = source # 为了在Milvus中建立文件到Minio文件路径的映射
        if file_extension == ".py":
            text_splitter = RecursiveCharacterTextSplitter.from_language(
            language=Language.PYTHON, chunk_size=1000, chunk_overlap=100
        )
        else:
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
        documents = text_splitter.split_documents(raw_documents)
        logger.debug("Successfully split file: %s", source)
        return documents
    else:
        return ValueError("Document type is undefined")


def load_model():
    ROOT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    embeddings = HuggingFaceBgeEmbeddings(
        model_name="BAAI/bge-large-zh",
        model_kwargs={"device": "cuda" if torch.cuda.is_available() else "cpu"},
        encode_kwargs={"normalize_embeddings": True},
        cache_folder = f"{ROOT_DIRECTORY}\\embeddings\\",
    )
    return embeddings


def insert_embeddings(docs, embeddings, collection_name):
    try:
        Milvus.from_documents(
            docs,
            embeddings,
            collection_name=collection_name,
            connection_args={"host": "127.0.0.1", "port": "19530"},
        )
        logger.debug("Successfully insert embeddings.")
    except Exception as e:
        logger.error(f"Failed to insert embeddings. \n{e}")
        return ("插入向量失败！")


class MilvusClient:
    def __init__(self):
        self.conn = connections.connect(uri="http://localhost:19530")
    
    def list_collections(self)-> list[str]:
        self.conn
        collections = utility.list_collections()
        return collections
    
    def query(self, collection_name, source)-> dict[str, list[int]]:
        # source: "selected_bucket/object"
        self.conn
        collection = Collection(collection_name)
        pks = collection.query(expr="source == '{}'".format(source))
        # pks: [{'pk': 1}, {'pk': 2}]
        pk_list = [pk['pk'] for pk in pks]
        source_pk = {source: pk_list}
        return source_pk

    def delete(self, collection_name, pk_list: list[int]):
        self.conn
        collection = Collection(collection_name)
        collection.delete(expr = "pk in {}".format(pk_list))

    def drop_collection(self, collection_name):
        self.conn
        utility.drop_collection(collection_name)

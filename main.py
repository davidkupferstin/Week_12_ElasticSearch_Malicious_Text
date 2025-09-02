import pandas as pd
from elasticsearch import Elasticsearch, helpers
import time

class Connection:
    def __init__(self):
        self.es = Elasticsearch('http://localhost:9200')
        while True:
            time.sleep(5)
            try:
                self.client = self.es.info()
                break
            except:
                pass

class Index:
    def __init__(self, index_name, index_mapping):
        self.con = Connection()
        while self.con.es is None:
            time.sleep(0.1)
        self.es = self.con.es
        self.index_name = index_name
        self.index_mapping = index_mapping
        self.index = self.index_check_creation()

    def index_check_creation(self):
        # if self.es.indices.exists(index=self.index_name):
        #     self.es.indices.delete(index=self.index_name)

        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name)
            self.es.indices.put_mapping(index=self.index_name, body=self.index_mapping)
        return self.es.indices.exists(index=self.index_name)

class DocumentsIndex:
    def __init__(self, index_name="tweets_injected", index_mapping=None):
        if index_mapping is None:
            index_mapping = {
                'properties': {
                    'TweetID': {
                        'type': 'binary'
                    },
                    'CreateDate': {
                        "type": "text",
                    },
                    'Antisemitic': {
                        'type': 'byte'
                    },
                    'text': {
                        'type': 'text'
                    }
                }
            }
        self.df = pd.read_csv("tweets_injected 3.csv")
        self.tweets_injected = self.df.to_dict(orient='records')
        self.index_name = index_name
        self.con_and_index = Index(self.index_name, index_mapping)
        while self.con_and_index.es is None:
            time.sleep(0.1)
        self.es = self.con_and_index.es
        self.index_name = self.con_and_index.index_name

    def indexes_documents(self):
        try:
            success, failed = helpers.bulk(self.es, self._generate_documents())
            print(f"Successfully indexed {success} documents, {failed} failed.")

            self.es.indices.refresh(index=self.index_name)
        except Exception as e:
            print(f"An error: {e}")

    def _generate_documents(self):
        for i, doc in enumerate(self.tweets_injected):
            yield {
                "_index": self.index_name,
                "_id": i,
                "TweetID": doc['TweetID'],
                "CreateDate": doc['CreateDate'],
                "Antisemitic": doc['Antisemitic'],
                "text": doc['text']
            }

# class SentimentProcessing:
#     def __init__(self):
#         self.model_body = {
#     "model_type": "text_classification",
#     "inference_config": {
#         "text_classification": {
#             "labels": ["NEGATIVE", "POSITIVE", "NEUTRAL"]
#         }
#     }
# }
#         self.doc_index = DocumentsIndex()
#         self.es = self.doc_index.es
#         self.model_name = self.es.ml.put_trained_model(model_id="distilbert-sentiment", body=self.model_body)
#         self.pipeline_body = {
#     "description": "Analyze sentiment of text",
#     "processors": [
#         {
#             "inference": {
#                 "model_id": "distilbert-sentiment",
#                 "field_map": {
#                     "text": "text"
#                 },
#                 "target_field": "sentiment"
#             }
#         }
#     ]
# }
#         self.pipeline = self.es.ingest.put_pipeline(id="sentiment-analysis", body=self.pipeline_body)
#
#     def found_sentimeent_and_update(self):
#         self.es.update_by_query(
#             index="tweets_injected",
#             body={
#                 "script": {
#                     "source": "ctx._source = ctx._source",
#                     "lang": "painless"
#                 },
#                 "query": {"match_all": {}},
#             },
#             pipeline="sentiment-analysis"
#         )

class SentimentProcessing:
    def __init__(self):
        self.doc_index = DocumentsIndex()
        self.es = self.doc_index.es

        self.model_id = "sentiment-analysis"

        self.pipeline_body = {
            "description": "Analyze sentiment of text",
            "processors": [
                {
                    "inference": {
                        "model_id": self.model_id,
                        "field_map": {
                            "text": "text"
                        },
                        "target_field": "sentiment"
                    }
                }
            ]
        }

        self.pipeline = self.es.ingest.put_pipeline(
            id="sentiment-analysis",
            body=self.pipeline_body
        )

    def found_sentiment_and_update(self):
        self.es.update_by_query(
            index="tweets_injected",
            body={
                "script": {
                    "source": "ctx._source = ctx._source",
                    "lang": "painless"
                },
                "query": {"match_all": {}},
            },
            pipeline="sentiment-analysis",
            max_docs = 10,
        )


class WeaponProcessing:
    def __init__(self):
        self.weapons_list = self._get_weapons(weapon_list_path="data/weapons_list.txt")
        self.query = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"text": word}} for word in self.weapons_list
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        self.doc_index = DocumentsIndex()
        while self.doc_index.es is None:
            time.sleep(0.1)
        self.es = self.doc_index.es
        self.index_name = self.doc_index.index_name
        self.documents = Retrieval(self.index_name, self.es).get_retrieval()

    def _get_weapons(self, weapon_list_path):
        weapon_list = []
        with open(weapon_list_path, encoding='utf-8') as f:
            for weapon in f:
                weapon_list.append(weapon)
        return weapon_list

    def found_weapons_and_update(self):
        for hit in self.documents['hits']['hits']:
            doc_id = hit['_id']
            text = hit['_source'].get('text', '').lower()

            found_weapons = [w for w in self.weapons_list if w.lower() in text]

            self.es.update(
                index=self.index_name,
                id=doc_id,
                body={"doc": {"weapons": found_weapons}}
            )

class Deletion:
    def __init__(self, query=None):
        if query is None:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "sentiment": ["positive", "neutral"]
                                }
                            }
                        ],
                        "must_not": [
                            {"exists": {"field": "weapons"}},
                            {"match": {"Antisemitic": "1"}}
                        ]
                    }
                }
            }
        self.doc_index = DocumentsIndex()
        while self.doc_index.es is None:
            time.sleep(0.1)
        self.es = self.doc_index.es
        self.index_name = self.doc_index.index_name
        self.query = query

    def delete_not_antisemitic(self):
        try:
            response = self.es.delete_by_query(
                index=self.index_name,
                body=self.query,
                wait_for_completion=True
            )
            print(response)
        except Exception as e:
            print(f"An error occurred: {e}")

class Retrieval:
    def __init__(self, index_name, es, query=None):
        if query is None:
            query = {"query": {"match_all": {}}}
        self.index_name = index_name
        self.es = es
        self.query = query

    def get_retrieval(self):
        resp = self.es.search(
            index=self.index_name,
            body=self.query
        )
        return resp

class Main:
    Connection()
    inx = DocumentsIndex()
    while inx.es is None:
        time.sleep(0.1)
    es = inx.es
    index_name = inx.index_name
    inx.indexes_documents()
    sen = SentimentProcessing()
    sen.found_sentiment_and_update()
    weap = WeaponProcessing()
    weap.found_weapons_and_update()
    dele = Deletion()
    dele.delete_not_antisemitic()
    antisemites_query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"Antisemitic": 1}},
                {"exists": {"field": "weapons_find"}}
            ]
        }
    }
}
    antisemites = Retrieval(index_name, es, antisemites_query)
    two_weapons_query = {
    "query": {
        "script": {
            "script": {
                "source": "doc['weapons'].size() >= params.min_count",
                "params": {"min_count": 2}
            }
        }
    }
}
    two_weapons = Retrieval(index_name, es, antisemites_query)

m = Main()
print(m.antisemites, m.two_weapons)








# for i in range(len(tweets_injected)):
#     try:
#         response = es.get(index=index_name, id=i)
#         if response["found"]:
#             print(f"Document found: {response['_source']}")
#         else:
#             print(f"Document with ID '{i}' not found in index '{index_name}'.")
#     except Exception as e:
#         print(f"Error retrieving document: {e}")

# results = es.search(index=index_name, body=query)
#
#
# print(f"Found {results['hits']['total']['value']} results.")
# for hit in results['hits']['hits']:
#     print(f"ID: {hit['_id']} - Category: {hit['_source']['category']}")
#     print(f"Text: {hit['_source']['text'][:200]}...\n")  # Print first 200 characters


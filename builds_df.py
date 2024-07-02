#!/usr/bin/env python
# coding: utf-8

# In[77]:


import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import 


# In[78]:


es=Elasticsearch([{"host":"localhost","port":9200}],http_auth=("elastic","suVlWmYRKKFk6RYs_TrU"))
print(es.ping())


# In[79]:


df=pd.read_csv('buildler.csv')


# In[80]:


df.dropna(inplace=True)


# In[81]:


path_of_exile_builds_index = {
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "custom_analyzer": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "asciifolding",
              "custom_edge_ngram"
            ]
          }
        },
        "filter": {
          "custom_edge_ngram": {
            "type": "edge_ngram",
            "min_gram": 2,
            "max_gram": 10
          }
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "Build": {
        "type": "text",
        "analyzer": "custom_analyzer"
      },
      "Ascendancy": {
        "type": "keyword"
      },
      "Açıklama": {
        "type": "text",
        "analyzer": "custom_analyzer"
      }
    }
  }
}


# In[82]:


def dataframe_to_es(df, es_index):
    for df_idx, line in df.iterrows():
        yield {
            "_index": es_index,
            "_id": df_idx,  # Satır numarasını Elasticsearch belgesi ID'si olarak kullanıyoruz
            "_source": {
                "Build": line['Build'],
                "Ascendancy": line['Ascendancy'],
                "Açıklama": line['Açıklama']
            }
        }


# In[83]:


try:
    es.indices.delete("path_of_exile_builds_index")
except:
    print("No index")


# In[84]:


helpers.bulk(es, dataframe_to_es(df, "path_of_exile_builds_index"), raise_on_error=False)


# In[89]:


response = es.search(index='path_of_exile_builds', body={
  "query": {
    "match": {
      "Build": "rea"
    }
  }
})

for hit in response['hits']['hits']:
    print(hit['_source'])


# In[90]:


connection.info()


# In[ ]:





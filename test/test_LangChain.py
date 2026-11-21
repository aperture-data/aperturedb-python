"""Optional LangChain integration with deterministic, local-only embeddings."""
import asyncio
import json
import os
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import numpy as np
import pytest

pytest.importorskip('langchain_core')
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

from aperturedb.LangChain import ApertureDB
from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest


class LocalEmbeddings(Embeddings):
    def embed_documents(self, texts):
        return [self.embed_query(text) for text in texts]

    def embed_query(self, text):
        return {'rice': [1., 0., 0.], 'soup': [0., 1., 0.],
                'beans': [.8, .2, 0.]}.get(text, [0., 0., 1.])


def store():
    result = ApertureDB.__new__(ApertureDB)
    result.embedding_function = LocalEmbeddings()
    result.dimensions = 3
    result.descriptor_class = 'Recipes'
    result._modern = True
    result.batch_size = 2
    result.connection = SimpleNamespace(hello_version=2)
    result._execute = Mock(return_value=([], []))
    return result


@pytest.mark.parametrize('ids,metadata', [(['a'], [{}, {}]), (['a', 'b'], [{}]),
                                          (['a', 'a'], [{}, {}]), (['', 'b'], [{}, {}])])
def test_invalid_upsert_inputs_cannot_delete_documents(ids, metadata):
    vectorstore = store()
    with pytest.raises(ValueError):
        vectorstore.add_texts(['rice', 'soup'], ids=ids, metadatas=metadata)
    vectorstore._execute.assert_not_called()


def test_invalid_embedding_vectors_cannot_delete_documents():
    vectorstore = store()
    vectorstore.embedding_function = Mock(
        embed_documents=lambda _: [[1., 2.], [3., 4.]])
    with pytest.raises(ValueError, match='configured dimensions'):
        vectorstore.add_texts(['rice', 'soup'], ids=['a', 'b'])
    vectorstore._execute.assert_not_called()


def test_upsert_has_one_atomic_delete_and_insert_transaction_per_batch():
    vectorstore = store()
    assert vectorstore.add_texts(['rice', 'soup', 'beans'], ids=[
                                 '$a', 'b', 'c']) == ['$a', 'b', 'c']
    assert vectorstore._execute.call_count == 2
    query, blobs = vectorstore._execute.call_args_list[0].args
    assert query[0]['DeleteDescriptor'] == {
        'with_class': 'Recipes', 'constraints': ['$uniqueid', 'in', {'array': ['$$a', 'b']}]}
    assert len(query) == 3 and len(blobs) == 2
    assert all(item['AddDescriptor']['class'] ==
               'Recipes' for item in query[1:])
    assert np.array_equal(np.frombuffer(
        blobs[0], dtype=np.float32), [1., 0., 0.])


def test_mixed_existing_and_missing_document_ids_are_supported():
    vectorstore = store()
    ids = vectorstore.add_documents([Document(page_content='rice', id='known'),
                                     Document(page_content='soup')])
    assert ids[0] == 'known' and isinstance(ids[1], str) and ids[1]


@pytest.mark.skipif(not os.getenv('ADB_COMPAT_ENDPOINTS'), reason='Set ADB_COMPAT_ENDPOINTS for live SDK tests')
def test_langchain_store_upsert_retriever_and_metadata():
    for endpoint in json.loads(os.environ['ADB_COMPAT_ENDPOINTS']):
        cls = ConnectorRest if endpoint.get('rest') else Connector
        client = cls(host=endpoint.get('host', '127.0.0.1'), port=endpoint['port'],
                     user=os.getenv('ADB_COMPAT_USERNAME', 'admin'),
                     password=os.getenv('ADB_COMPAT_PASSWORD', 'admin'),
                     use_ssl=endpoint.get('use_ssl', False), ca_cert=endpoint.get('ca_cert'),
                     verify_hostname=endpoint.get('verify_hostname', False))
        name = 'LangChainTest' + uuid4().hex
        vectorstore = None
        try:
            vectorstore = ApertureDB(embeddings=LocalEmbeddings(), descriptor_class=name,
                                     dimensions=3, engine='Flat', metric='CS', client=client,
                                     properties={'model': 'local-test'})
            assert vectorstore.add_texts(['rice', 'soup'], ids=['$rice', 'soup'],
                                         metadatas=[{'title': 'Rice', 'details': {'source': '$test', 'values': [1, None, {'k': 2}]}}, {'title': 'Soup'}]) == ['$rice', 'soup']
            docs = vectorstore.similarity_search('rice', k=2)
            assert docs[0].id == '$rice' and docs[0].metadata['title'] == 'Rice'
            assert docs[0].metadata['details']['values'] == [1, None, {'k': 2}]
            assert vectorstore.as_retriever(search_kwargs={'k': 1}).invoke('rice')[
                0].id == '$rice'
            assert vectorstore.similarity_search(
                'rice', k=2, filter=['$lc_title', '==', 'Soup'])[0].id == 'soup'
            assert vectorstore.as_retriever(search_type='mmr', search_kwargs={
                'k': 2, 'fetch_k': 2}).invoke('rice')[0].id == '$rice'
            vectorstore.add_documents(
                [Document(page_content='beans', id='$rice', metadata={'title': 'Beans'})])
            assert vectorstore.get_by_ids(['$rice'])[0].page_content == 'beans'
            assert len(vectorstore.similarity_search('rice', k=5)) == 2
            assert vectorstore.delete(['soup'])
            assert not vectorstore.get_by_ids(['soup'])
            reused = ApertureDB(embeddings=LocalEmbeddings(), descriptor_class=name,
                                client=client, properties={'model': 'local-test'})
            assert reused.dimensions == 3 and reused.metric == 'CS' and reused.engine == 'Flat'
            with pytest.raises(ValueError, match='metadata'):
                ApertureDB(embeddings=LocalEmbeddings(), descriptor_class=name,
                           client=client, properties={'model': 'different'})
            with pytest.raises(ValueError, match='dimensions'):
                ApertureDB(embeddings=LocalEmbeddings(), descriptor_class=name,
                           client=client, dimensions=5)
            # LangChain's default async wrappers use worker threads. Exercise
            # concurrent operations so separate SDK sockets are required.

            async def read_both():
                return await asyncio.gather(vectorstore.aget_by_ids(['$rice']),
                                            vectorstore.aget_by_ids(['$rice']))
            assert all(
                rows[0].id == '$rice' for rows in asyncio.run(read_both()))

            async def search_concurrently():
                return await asyncio.gather(
                    *[vectorstore.asimilarity_search('rice', k=1)
                      for _ in range(4)],
                    *[vectorstore.amax_marginal_relevance_search('rice', k=1, fetch_k=2) for _ in range(2)])
            assert all(rows[0].id == '$rice' for rows in asyncio.run(
                search_concurrently()))
            listed = vectorstore.list_vectorstores(client=client)
            assert any(row.get('class', row.get('_name'))
                       == name for row in listed)
        finally:
            if vectorstore is not None:
                vectorstore.delete_vectorstore(name, client=client)
            client.close()

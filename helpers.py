from tusclient import client
from io import BufferedReader
from sklearn.metrics.pairwise import euclidean_distances
from ast import literal_eval
from mistralai import Mistral


def upload_file_to_supabase(
    supabase_url:str, file_name: str, file: BufferedReader, access_token: str
):
    # create Tus client
    my_client = client.TusClient(
        f"{supabase_url}/storage/v1/upload/resumable",
        headers={"Authorization": f"Bearer {access_token}", "x-upsert": "true"},
    )
    uploader = my_client.uploader(
        file_stream=file,
        chunk_size=(6 * 1024 * 1024),
        metadata={
            "bucketName": "document-pdfs",
            "objectName": file_name,
            "contentType": "application/pdf",
            "cacheControl": "3600",
        },
    )
    uploader.upload()


def get_most_similar_pages(prompt: str, pages: list, mistral_api_key, topk=5):
    """ Embed prompt with Mistral, compare with all supplied pages and return topk """
    client = Mistral(api_key=mistral_api_key)
    embeddings_response = client.embeddings.create(
        model="mistral-embed",
        inputs=prompt
    )
    prompt_emb = embeddings_response.data[0].embedding

    for page in pages:
        distance = euclidean_distances([literal_eval(page["embedding"])], [prompt_emb])

        page["score"] = distance[0][0]

    pages = sorted(pages, key=lambda x: x["score"], reverse=True)
    pages = pages[:topk]
    
    return pages


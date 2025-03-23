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


def get_batches(n, batch_size):
    chunks = []
    for i in range(0, n, batch_size):
        chunks.append(list(range(n))[i:i + batch_size])

    return chunks


def upload_file_to_mistral_ocr(path: str, mistral_api_key):
    """ returns signed mistral url to use for ocr"""
    client = Mistral(api_key=mistral_api_key)

    with open(path, "rb") as f:
        uploaded_pdf = client.files.upload(
            file={
                "file_name": "report-pdf-pages",
                "content": f,
            },
            purpose="ocr"
        )

    signed_url = client.files.get_signed_url(file_id=uploaded_pdf.id)

    ocr_response = client.ocr.process(
        model="mistral-ocr-latest",
        document={
            "type": "document_url",
            "document_url": signed_url.url,
        }
    )

    return ocr_response


def insert_page_to_supabase(supabase, document_id, page, content, embedding):
    (
        supabase.table("pages")
        .upsert(
            {
                "document_id": document_id,
                "page": page,
                "content": content,
                "embedding": embedding
            }
        )
        .execute()
    )


def create_embedding(text: str, mistral_api_key):
    client = Mistral(api_key=mistral_api_key)
    try:
        embeddings_response = client.embeddings.create(
            model="mistral-embed",
            inputs=text,
        )

    except:
        embeddings_response = ...

    return embeddings_response

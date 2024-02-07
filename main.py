from PyPDF2 import PdfReader
import requests

class PdfDataInsertion:

    def __init__(self, vectorize_model: str = "sbert"):
        self.vectorize_model = vectorize_model

    @staticmethod
    def extract_tokens(pdf_path: str, chunk_size: int = 100) -> list:
        with open(pdf_path, 'rb') as file:
            pdf = PdfReader(file)
            text = ""
            for page in pdf.pages:
                text += page.extract_text()

            # Split text into lines
            lines = text.split('\n')

            # Create a list to store chunks
            token_chunks = []

            # Collect tokens in chunks of 100 or at each newline
            for line in lines:
                tokens = line.split()
                for i in range(0, len(tokens), chunk_size):
                    chunk_string = " ".join(tokens[i:i + chunk_size])
                    token_chunks.append(chunk_string)
            return token_chunks

    def vectorize_text(self, pdf_path: str, chunk_size: int = 100) -> list[float]:
        token_chunks = self.extract_tokens(pdf_path, chunk_size)
        vectors = []

        for chunk in token_chunks:
            try:
                response = requests.post(
                    "http://172.105.247.6:6000/vectorize",
                    json={'element': chunk, 'model': self.vectorize_model}
                )
                response.raise_for_status()
                result = response.json()['vector']
                print(result)
                vectors.append(result)
            except requests.exceptions.RequestException as e:
                print(f"Error in vectorizing: {e}")

        return vectors, token_chunks

    def insert_data(self, pdf_path: str, chunk_size: int, collection_name: str):
        vectors, chunks = self.vectorize_text(pdf_path, chunk_size)

        for vector, chunk in zip(vectors, chunks):
            try:
                response = requests.post(
                    "http://172.105.247.6:6000/insert_single_point",
                    json={"vector": vector, "collection": collection_name, "text": chunk}
                )
                response.raise_for_status()
                print(response.json())
            except requests.exceptions.RequestException as e:
                print(f"Error in inserting data: {e}")

# Example usage
pdf_path = "/content/excell.pdf"
chunk_size = 2000
collection_name = "new-model-test"
vectorize_model = "sbert"
edi = PdfDataInsertion(vectorize_model)
edi.insert_data(pdf_path, chunk_size, collection_name)


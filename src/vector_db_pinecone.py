"""
Lab 4 - Vector DB & RAG Index with Pinecone
Creates embeddings and stores in Pinecone for cloud-ready RAG
"""

import os
from pathlib import Path
from typing import List, Dict, Optional
import tiktoken
from openai import OpenAI
from pinecone import Pinecone, ServerlessSpec
import time
from dotenv import load_dotenv

from src.utils import (
    logger,
    load_json,
    save_json,
    ScraperConfig,
    get_timestamp
)

# Load environment
load_dotenv()


class TextChunker:
    """Chunk text into semantic pieces for embedding"""
    
    def __init__(self, chunk_size: int = 800, overlap: int = 100):
        """
        Args:
            chunk_size: Target tokens per chunk (500-1000 recommended)
            overlap: Tokens to overlap between chunks for context
        """
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.encoder = tiktoken.get_encoding("cl100k_base")  # GPT-4 encoding
    
    def count_tokens(self, text: str) -> int:
        """Count tokens in text"""
        return len(self.encoder.encode(text))
    
    def chunk_text(self, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Split text into overlapping chunks
        Returns list of {text, metadata, token_count}
        """
        if not text or len(text) < 50:
            return []
        
        # Split into sentences first (rough approximation)
        sentences = text.replace('\n', ' ').split('. ')
        
        chunks = []
        current_chunk = []
        current_tokens = 0
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            sentence_tokens = self.count_tokens(sentence)
            
            # If adding this sentence exceeds chunk size, save current chunk
            if current_tokens + sentence_tokens > self.chunk_size and current_chunk:
                chunk_text = '. '.join(current_chunk) + '.'
                chunks.append({
                    'text': chunk_text,
                    'metadata': metadata or {},
                    'token_count': current_tokens
                })
                
                # Start new chunk with overlap
                overlap_sentences = []
                overlap_tokens = 0
                for s in reversed(current_chunk):
                    s_tokens = self.count_tokens(s)
                    if overlap_tokens + s_tokens < self.overlap:
                        overlap_sentences.insert(0, s)
                        overlap_tokens += s_tokens
                    else:
                        break
                
                current_chunk = overlap_sentences
                current_tokens = overlap_tokens
            
            current_chunk.append(sentence)
            current_tokens += sentence_tokens
        
        # Add final chunk
        if current_chunk:
            chunk_text = '. '.join(current_chunk) + '.'
            chunks.append({
                'text': chunk_text,
                'metadata': metadata or {},
                'token_count': current_tokens
            })
        
        return chunks


class PineconeVectorDB:
    """Build and manage Pinecone vector database"""
    
    def __init__(self, 
                 index_name: str = "pe-dashboard-ai50",
                 embedding_model: str = "text-embedding-3-small",
                 dimension: int = 1536):
        """
        Initialize Pinecone connection
        
        Args:
            index_name: Name for Pinecone index
            embedding_model: OpenAI embedding model
            dimension: Embedding dimension (1536 for text-embedding-3-small)
        """
        
        # Get API keys
        pinecone_api_key = os.getenv("PINECONE_API_KEY")
        openai_api_key = os.getenv("OPENAI_API_KEY")
        
        if not pinecone_api_key:
            raise ValueError("PINECONE_API_KEY not found in .env")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY not found in .env")
        
        # Initialize clients
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.openai_client = OpenAI(api_key=openai_api_key)
        
        self.index_name = index_name
        self.embedding_model = embedding_model
        self.dimension = dimension
        self.chunker = TextChunker(chunk_size=800, overlap=100)
        
        logger.info(f"Initialized Pinecone with index: {index_name}")
    
    def create_index(self):
        """Create Pinecone index if it doesn't exist"""
        
        # Check if index exists
        existing_indexes = self.pc.list_indexes()
        
        if self.index_name in [idx.name for idx in existing_indexes]:
            logger.info(f"Index '{self.index_name}' already exists")
            
            # Ask if user wants to delete and recreate
            response = input(f"Delete and recreate index '{self.index_name}'? (y/n): ")
            if response.lower() == 'y':
                logger.info(f"Deleting existing index...")
                self.pc.delete_index(self.index_name)
                time.sleep(5)  # Wait for deletion
            else:
                logger.info("Using existing index")
                self.index = self.pc.Index(self.index_name)
                return
        
        # Create new index
        logger.info(f"Creating new index '{self.index_name}'...")
        
        self.pc.create_index(
            name=self.index_name,
            dimension=self.dimension,
            metric="cosine",
            spec=ServerlessSpec(
                cloud="aws",
                region="us-east-1"  # Free tier region
            )
        )
        
        # Wait for index to be ready
        logger.info("Waiting for index to be ready...")
        while not self.pc.describe_index(self.index_name).status['ready']:
            time.sleep(1)
        
        self.index = self.pc.Index(self.index_name)
        logger.info(f"✓ Index created and ready")
    
    def get_embedding(self, text: str) -> List[float]:
        """Get embedding vector for text"""
        try:
            response = self.openai_client.embeddings.create(
                model=self.embedding_model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error getting embedding: {e}")
            return None
    
    def batch_embed(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
        """Embed multiple texts in batches"""
        embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            try:
                response = self.openai_client.embeddings.create(
                    model=self.embedding_model,
                    input=batch
                )
                batch_embeddings = [r.embedding for r in response.data]
                embeddings.extend(batch_embeddings)
                
                logger.info(f"Embedded {len(embeddings)}/{len(texts)} chunks")
                
            except Exception as e:
                logger.error(f"Error in batch embedding: {e}")
                # Fallback to individual
                for text in batch:
                    emb = self.get_embedding(text)
                    if emb:
                        embeddings.append(emb)
        
        return embeddings
    
    def process_company(self, company_id: str) -> List[Dict]:
        """Process one company and return chunks"""
        
        company_folder = ScraperConfig.RAW_DIR / company_id / "initial"
        
        if not company_folder.exists():
            # Try old structure
            company_folder = ScraperConfig.RAW_DIR / company_id
            run_folders = sorted(company_folder.glob("full_*"), reverse=True)
            if not run_folders:
                logger.warning(f"No data found for {company_id}")
                return []
            company_folder = run_folders[0]
        
        company_chunks = []
        
        # If initial/ folder, merge all timestamp subfolders
        if company_folder.name == "initial":
            for timestamp_folder in company_folder.iterdir():
                if not timestamp_folder.is_dir():
                    continue
                
                for txt_file in timestamp_folder.glob("*.txt"):
                    page_type = txt_file.stem
                    
                    if 'metadata' in page_type or 'growth_momentum' in page_type:
                        continue
                    
                    try:
                        with open(txt_file, 'r', encoding='utf-8', errors='ignore') as f:
                            text = f.read()
                        
                        if len(text) > 100:
                            chunks = self.chunker.chunk_text(
                                text,
                                metadata={
                                    'company_id': company_id,
                                    'page_type': page_type,
                                    'source_file': str(txt_file)
                                }
                            )
                            company_chunks.extend(chunks)
                    
                    except Exception as e:
                        logger.warning(f"Could not process {txt_file}: {e}")
        else:
            # Single folder structure
            for txt_file in company_folder.glob("*_clean.txt"):
                page_type = txt_file.stem.replace('_clean', '')
                
                try:
                    with open(txt_file, 'r', encoding='utf-8', errors='ignore') as f:
                        text = f.read()
                    
                    if len(text) > 100:
                        chunks = self.chunker.chunk_text(
                            text,
                            metadata={
                                'company_id': company_id,
                                'page_type': page_type,
                                'source_file': str(txt_file)
                            }
                        )
                        company_chunks.extend(chunks)
                
                except Exception as e:
                    logger.warning(f"Could not process {txt_file}: {e}")
        
        logger.info(f"  {company_id}: {len(company_chunks)} chunks")
        return company_chunks
    
    def upsert_chunks(self, chunks: List[Dict], batch_size: int = 100):
        """Upload chunks to Pinecone"""
        
        logger.info(f"\nEmbedding {len(chunks)} chunks...")
        
        # Get embeddings
        texts = [chunk['text'] for chunk in chunks]
        embeddings = self.batch_embed(texts, batch_size=100)
        
        logger.info(f"\nUpserting to Pinecone...")
        
        # Prepare vectors for Pinecone
        vectors = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            vector_id = f"{chunk['metadata']['company_id']}_{i}"
            
            vectors.append({
                'id': vector_id,
                'values': embedding,
                'metadata': {
                    'company_id': chunk['metadata']['company_id'],
                    'page_type': chunk['metadata']['page_type'],
                    'text': chunk['text'][:1000],  # Pinecone metadata limit
                    'token_count': chunk.get('token_count', 0)
                }
            })
        
        # Upsert in batches
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            self.index.upsert(vectors=batch)
            logger.info(f"Upserted {min(i + batch_size, len(vectors))}/{len(vectors)} vectors")
        
        logger.info(f"✓ All vectors uploaded to Pinecone")
    
    def build_for_all_companies(self):
        """Build vector DB from all companies"""
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"LAB 4 - BUILDING PINECONE VECTOR DATABASE")
        logger.info(f"{'#'*60}\n")
        
        # Create index
        self.create_index()
        
        # Process all companies
        raw_dir = ScraperConfig.RAW_DIR
        company_folders = sorted([f for f in raw_dir.iterdir() if f.is_dir()])
        
        logger.info(f"\nProcessing {len(company_folders)} companies...\n")
        
        all_chunks = []
        for idx, company_folder in enumerate(company_folders, 1):
            company_id = company_folder.name
            logger.info(f"[{idx}/{len(company_folders)}] Processing {company_id}...")
            
            chunks = self.process_company(company_id)
            all_chunks.extend(chunks)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Chunking complete: {len(all_chunks)} total chunks")
        logger.info(f"{'='*60}\n")
        
        # Upload to Pinecone
        if all_chunks:
            self.upsert_chunks(all_chunks)
        
        # Save summary locally
        summary = {
            'created_at': get_timestamp(),
            'total_chunks': len(all_chunks),
            'embedding_model': self.embedding_model,
            'index_name': self.index_name,
            'chunk_size': self.chunker.chunk_size,
            'overlap': self.chunker.overlap
        }
        
        summary_file = ScraperConfig.DATA_DIR / "pinecone_vector_db_info.json"
        save_json(summary, summary_file)
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"PINECONE VECTOR DB BUILD COMPLETE")
        logger.info(f"Index: {self.index_name}")
        logger.info(f"Total chunks: {len(all_chunks)}")
        logger.info(f"{'#'*60}\n")


class PineconeRetriever:
    """Retrieve relevant chunks from Pinecone"""
    
    def __init__(self, 
                 index_name: str = "pe-dashboard-ai50",
                 embedding_model: str = "text-embedding-3-small"):
        """Load Pinecone index"""
        
        pinecone_api_key = os.getenv("PINECONE_API_KEY")
        openai_api_key = os.getenv("OPENAI_API_KEY")
        
        if not pinecone_api_key:
            raise ValueError("PINECONE_API_KEY not found in .env")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY not found in .env")
        
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.openai_client = OpenAI(api_key=openai_api_key)
        
        self.index = self.pc.Index(index_name)
        self.embedding_model = embedding_model
        
        # Get index stats
        stats = self.index.describe_index_stats()
        logger.info(f"Connected to Pinecone index: {index_name}")
        logger.info(f"Total vectors: {stats['total_vector_count']}")
    
    def search(self, 
               query: str, 
               company_id: str = None, 
               k: int = 5,
               page_types: List[str] = None) -> List[Dict]:
        """
        Search for relevant chunks
        
        Args:
            query: Search query
            company_id: Optional company filter
            k: Number of results
            page_types: Optional list of page types to filter (e.g., ['blog', 'homepage'])
        
        Returns:
            List of {text, metadata, score} dicts
        """
        
        # Get query embedding
        response = self.openai_client.embeddings.create(
            model=self.embedding_model,
            input=query
        )
        query_vector = response.data[0].embedding
        
        # Build filter
        filter_dict = {}
        if company_id:
            filter_dict['company_id'] = company_id
        if page_types:
            filter_dict['page_type'] = {'$in': page_types}
        
        # Search Pinecone
        results = self.index.query(
            vector=query_vector,
            top_k=k,
            include_metadata=True,
            filter=filter_dict if filter_dict else None
        )
        
        # Format results
        formatted = []
        for match in results['matches']:
            formatted.append({
                'text': match['metadata'].get('text', ''),
                'metadata': {
                    'company_id': match['metadata'].get('company_id'),
                    'page_type': match['metadata'].get('page_type'),
                    'token_count': match['metadata'].get('token_count')
                },
                'score': match['score']
            })
        
        return formatted


def build_pinecone_db():
    """Build Pinecone vector DB from all companies"""
    
    builder = PineconeVectorDB()
    builder.build_for_all_companies()


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Lab 4 - Pinecone Vector DB")
    parser.add_argument('--build', action='store_true', help='Build vector DB')
    parser.add_argument('--test', action='store_true', help='Test search')
    parser.add_argument('--query', type=str, help='Search query')
    parser.add_argument('--company', type=str, help='Filter by company')
    parser.add_argument('--index-name', type=str, default='pe-dashboard-ai50', help='Pinecone index name')
    
    args = parser.parse_args()
    
    if args.build:
        build_pinecone_db()
    
    if args.test or args.query:
        # Test retrieval
        retriever = PineconeRetriever(index_name=args.index_name)
        
        query = args.query or "What is the company's funding history?"
        company = args.company
        
        logger.info(f"\n{'='*60}")
        logger.info(f"SEARCH TEST")
        logger.info(f"{'='*60}")
        logger.info(f"Query: '{query}'")
        if company:
            logger.info(f"Company filter: {company}")
        logger.info(f"{'='*60}\n")
        
        results = retriever.search(query, company_id=company, k=5)
        
        for i, result in enumerate(results, 1):
            logger.info(f"Result {i}:")
            logger.info(f"  Company: {result['metadata'].get('company_id')}")
            logger.info(f"  Page: {result['metadata'].get('page_type')}")
            logger.info(f"  Score: {result['score']:.4f}")
            logger.info(f"  Text: {result['text'][:200]}...")
            logger.info("")
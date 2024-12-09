import os
import re
import arxiv
import datetime
import requests
import csv
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Set
from bs4 import BeautifulSoup

from dotenv import load_dotenv

load_dotenv()

class ArxivFilter:
    """論文のフィルタリングを行うクラス"""
    
    def __init__(self, keywords: List[str]):
        self.keywords = [keyword.lower() for keyword in keywords]
    
    def matches_keywords(self, abstract: str) -> bool:
        abstract_lower = abstract.lower()
        return any(keyword in abstract_lower for keyword in self.keywords)
    
    def is_published_yesterday(self, published_date: datetime.datetime) -> bool:
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        return published_date.date() == yesterday

class ArxivFetcher:
    def __init__(self, 
                 keywords: List[str],
                 max_results: int = 100,
                 sort_by: arxiv.SortCriterion = arxiv.SortCriterion.SubmittedDate):
        self.filter = ArxivFilter(keywords)
        self.max_results = max_results
        self.sort_by = sort_by
        self.keywords = keywords
        
    def fetch_papers(self) -> List[Dict]:
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        date_str = yesterday.strftime("%Y%m%d")
        next_date = yesterday + datetime.timedelta(days=1)
        next_date_str = next_date.strftime("%Y%m%d")
        
        query = f'cat:cs.LG AND submittedDate:[{date_str}0000 TO {next_date_str}0000]'
        
        client = arxiv.Client(
            page_size=100,
            delay_seconds=3.0,
            num_retries=5
        )
        
        search = arxiv.Search(
            query=query,
            max_results=self.max_results,
            sort_by=self.sort_by
        )
        
        filtered_papers = []
        for result in client.results(search):
            if not self.filter.matches_keywords(result.summary):
                continue
            if not self.filter.is_published_yesterday(result.published):
                continue
            
            paper_info = {
                'title': result.title,
                'authors': [author.name for author in result.authors],
                'summary': result.summary,
                'pdf_url': result.pdf_url,
                'entry_id': result.entry_id,
                'published': result.published.strftime("%Y-%m-%d %H:%M:%S"),
                'updated': result.updated.strftime("%Y-%m-%d %H:%M:%S"),
                'categories': result.categories,
                'keywords': self.keywords
            }
            filtered_papers.append(paper_info)
            
        return filtered_papers

class PaperStorage(ABC):
    """論文情報の保存インターフェース"""
    
    @abstractmethod
    def save_paper(self, paper: Dict) -> None:
        """個別の論文情報を保存"""
        pass
    
    @abstractmethod
    def get_existing_paper_urls(self) -> Set[str]:
        """既存の論文URLを取得"""
        pass

class NotionStorage(PaperStorage):
    """Notionへの保存を行うクラス"""
    
    def __init__(self, token: str, database_id: str):
        self.token = token
        self.database_id = database_id
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        self.base_url = "https://api.notion.com/v1"
    
    def save_paper(self, paper: Dict) -> None:
        url = f"{self.base_url}/pages"
        
        properties = {
            "Title": {
                "title": [{"type": "text", "text": {"content": paper['title']}}]
            },
            "Paper URL": {
                "url": paper['paper_url']
            },
            "GitHub URL": {
                "url": paper.get('github_url')
            },
            "Published Date": {
                "date": {
                    "start": paper['published'].split()[0]
                }
            },
            "Keywords": {
                "multi_select": [
                    {"name": keyword} for keyword in paper.get('keywords', [])
                ]
            }
        }
        
        payload = {
            "parent": {"database_id": self.database_id},
            "properties": properties
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
    
    def get_existing_paper_urls(self) -> Set[str]:
        existing_urls = set()
        has_more = True
        start_cursor = None
        
        while has_more:
            url = f"{self.base_url}/databases/{self.database_id}/query"
            payload = {}
            if start_cursor:
                payload["start_cursor"] = start_cursor
                
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            data = response.json()
            
            for page in data["results"]:
                paper_url = page["properties"]["Paper URL"]["url"]
                if paper_url:
                    existing_urls.add(paper_url)
            
            has_more = data["has_more"]
            if has_more:
                start_cursor = data["next_cursor"]
                
        return existing_urls

class CsvStorage(PaperStorage):
    """CSVへの保存を行うクラス"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """CSVファイルが存在しない場合、ヘッダーを書き込んで作成"""
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, 
                    fieldnames=['title', 'paper_url', 'github_url', 'published', 'keywords'])
                writer.writeheader()
    
    def save_paper(self, paper: Dict) -> None:
        with open(self.csv_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, 
                fieldnames=['title', 'paper_url', 'github_url', 'published', 'keywords'])
            writer.writerow({
                'title': paper['title'],
                'paper_url': paper['pdf_url'],
                'github_url': paper.get('github_url', ''),
                'published': paper['published'],
                'keywords': ', '.join(paper['keywords'])
            })
    
    def get_existing_paper_urls(self) -> Set[str]:
        existing_urls = set()
        if not os.path.exists(self.csv_path):
            return existing_urls
            
        with open(self.csv_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['paper_url']:
                    existing_urls.add(row['paper_url'])
        return existing_urls

class ArxivPaperProcessor:
    """論文情報の処理を行うクラス"""
    
    def __init__(self, storage: PaperStorage):
        self.storage = storage
    
    @staticmethod
    def extract_github_url(pdf_url: str) -> Optional[str]:
        """論文のHTMLページからGitHubのURLを抽出"""
        html_url = pdf_url.replace('/pdf/', '/html/')

        try:
            response = requests.get(html_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            github_pattern = r'https?://github\.com/[^\s<>"\.]+'

            # Abstract内を検索
            abstract_div = soup.find('div', class_='ltx_abstract')
            if abstract_div:
                github_urls = re.findall(github_pattern, abstract_div.get_text())
                if github_urls:
                    return github_urls[0]

            # Introduction内を検索
            section_s1 = soup.find('section', id='S1')
            if section_s1:
                github_urls = re.findall(github_pattern, section_s1.get_text())
                if github_urls:
                    return github_urls[0]

        except Exception as e:
            print(f"Warning: Failed to extract GitHub URL from {html_url}: {e}")
        
        return None
    
    def process_papers(self, papers: List[Dict]):
        """論文情報を処理して保存"""
        existing_urls = self.storage.get_existing_paper_urls()
        
        for paper in papers:
            paper_url = paper['pdf_url']
            
            if paper_url in existing_urls:
                print(f"Skipping existing paper: {paper['title']}")
                continue
            
            # GitHubのURLを抽出
            github_url = self.extract_github_url(paper_url)
            paper['github_url'] = github_url
            
            try:
                self.storage.save_paper(paper)
                print(f"Saved paper: {paper['title']}")
            except Exception as e:
                print(f"Error saving paper: {paper['title']}, Error: {e}")

def main():
    default_save_to = "csv"
    save_to = input(f"Enter save destination (notion/csv) [Or press Enter for default: {default_save_to}]: ").strip().lower()
    if not save_to:
        save_to = default_save_to
    
    if save_to not in ["notion", "csv"]:
        raise ValueError("Invalid save destination. Choose 'notion' or 'csv'.")
    
    keywords_str = input("Enter keywords separated by commas: ")
    keywords = [k.strip() for k in keywords_str.split(",")]
    print(f"Filtering papers with keywords: {keywords}")
    
    # 論文の取得
    fetcher = ArxivFetcher(keywords=keywords, max_results=1000)
    papers = fetcher.fetch_papers()
    print(f"Found {len(papers)} papers matching the criteria")
    
    # ストレージの初期化
    if save_to == "notion":
        notion_token = os.getenv("NOTION_TOKEN")
        notion_database_id = os.getenv("NOTION_DATABASE_ID")
        if not notion_token or not notion_database_id:
            raise ValueError("NOTION_TOKEN and NOTION_DATABASE_ID environment variables are required for Notion.")
        storage = NotionStorage(notion_token, notion_database_id)
    else:  # csv
        csv_path = os.getenv("CSV_PATH")
        if not csv_path:
            raise ValueError("CSV_PATH environment variable is required for saving to CSV.")
        storage = CsvStorage(csv_path)
    
    # 論文の処理と保存
    processor = ArxivPaperProcessor(storage)
    processor.process_papers(papers)

if __name__ == "__main__":
    main()
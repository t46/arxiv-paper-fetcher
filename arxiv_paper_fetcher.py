import os
import re
import arxiv
import datetime
import requests
from typing import List, Dict, Optional, Set
from bs4 import BeautifulSoup

from dotenv import load_dotenv

load_dotenv()

class ArxivFilter:
    """論文のフィルタリングを行うクラス"""
    
    def __init__(self, keywords: List[str]):
        """
        Args:
            keywords: アブストラクト内で検索するキーワードのリスト
        """
        self.keywords = [keyword.lower() for keyword in keywords]
    
    def matches_keywords(self, abstract: str) -> bool:
        """
        論文のアブストラクトが指定されたキーワードを含むかチェック
        
        Args:
            abstract: 論文のアブストラクト
            
        Returns:
            キーワードを含む場合はTrue
        """
        abstract_lower = abstract.lower()
        return any(keyword in abstract_lower for keyword in self.keywords)
    
    def is_published_yesterday(self, published_date: datetime.datetime) -> bool:
        """
        論文が昨日投稿されたかチェック
        
        Args:
            published_date: 論文の投稿日時
            
        Returns:
            昨日投稿された場合はTrue
        """
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        return published_date.date() == yesterday

class ArxivFetcher:
    def __init__(self, 
                 keywords: List[str],
                 max_results: int = 100,
                 sort_by: arxiv.SortCriterion = arxiv.SortCriterion.SubmittedDate):
        """
        Args:
            keywords: フィルタリングに使用するキーワードのリスト
            max_results: 1回の取得で取得する最大論文数
            sort_by: ソート基準
        """
        self.filter = ArxivFilter(keywords)
        self.max_results = max_results
        self.sort_by = sort_by
        self.keywords = keywords  # キーワードを保持
        
    def fetch_papers(self) -> List[Dict]:
        """
        昨日投稿されたcs.LG論文のうち、指定されたキーワードを含むものを取得
        
        Returns:
            フィルタリングされた論文情報のリスト
        """
        # 昨日の日付でクエリを構築
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        date_str = yesterday.strftime("%Y%m%d")
        next_date = yesterday + datetime.timedelta(days=1)
        next_date_str = next_date.strftime("%Y%m%d")
        
        query = f'cat:cs.LG AND submittedDate:[{date_str}0000 TO {next_date_str}0000]'
        
        # arxiv APIでの検索実行
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
            # キーワードでフィルタリング
            if not self.filter.matches_keywords(result.summary):
                continue
                
            # 投稿日でフィルタリング
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
                'keywords': self.keywords  # キーワードを保持
            }
            filtered_papers.append(paper_info)
            
        return filtered_papers

class NotionClient:
    def __init__(self, token: str, database_id: str):
        """
        Notion APIクライアントの初期化
        
        Args:
            token: Notion API トークン
            database_id: 論文情報を格納するデータベースのID
        """
        self.token = token
        self.database_id = database_id
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        self.base_url = "https://api.notion.com/v1"
        
    def create_page(self, paper_info: Dict) -> Dict:
        """
        論文情報をNotionデータベースに追加
        
        Args:
            paper_info: 論文情報の辞書
        
        Returns:
            作成されたページの情報
        """
        url = f"{self.base_url}/pages"
        
        # Notionページのプロパティを構築
        properties = {
            "Title": {
                "title": [
                    {
                        "type": "text",
                        "text": {
                            "content": paper_info['title']
                        }
                    }
                ]
            },
            "Paper URL": {
                "url": paper_info['paper_url']
            },
            "GitHub URL": {  # GitHub URLをURL型として設定
                "url": paper_info.get('github_url', None)
            },
            "Published Date": {
                "date": {
                    "start": paper_info['published'].split()[0]  # YYYY-MM-DD 形式に変換
                }
            },
            "Keywords": {
                "multi_select": [
                    {"name": keyword} for keyword in paper_info.get('keywords', [])
                ]
            }
        }
        
        payload = {
            "parent": {"database_id": self.database_id},
            "properties": properties
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def get_existing_paper_urls(self) -> Set[str]:
        """
        既存の論文URLを取得
        
        Returns:
            データベースに存在する論文URLのセット
        """
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

class ArxivPaperProcessor:
    def __init__(self, notion_client: NotionClient):
        """
        arxiv論文処理クラスの初期化
        
        Args:
            notion_client: Notionクライアントのインスタンス
        """
        self.notion_client = notion_client
        
    def extract_github_url(self, abstract: str, pdf_url: str) -> Optional[str]:
        """
        論文のアブストラクトからGitHubのURLを抽出
        
        Args:
            abstract: 論文のアブストラクト
            pdf_url: 論文のPDF URL
        
        Returns:
            抽出されたGitHub URL（見つからない場合はNone）
        """
        # アブストラクトからGitHub URLを抽出
        github_pattern = r'https?://github\.com/[a-zA-Z0-9-]+/[a-zA-Z0-9._-]+'
        github_urls = re.findall(github_pattern, abstract)
        
        if github_urls:
            return github_urls[0]  # 最初に見つかったURLを返す
            
        try:
            # arxiv URLからpdfを除いてabstractページのURLを作成
            abstract_url = pdf_url.replace('.pdf', '')
            response = requests.get(abstract_url)
            response.raise_for_status()
            
            # BeautifulSoupでHTMLをパース
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 論文ページ全体からGitHub URLを抽出
            for link in soup.find_all('a'):
                href = link.get('href', '')
                if 'github.com' in href:
                    match = re.search(github_pattern, href)
                    if match:
                        return match.group()
        except Exception as e:
            print(f"Warning: Failed to extract GitHub URL from paper page: {e}")
        
        return None

    def process_papers(self, papers: List[Dict]):
        """
        論文情報を処理してNotionデータベースに格納
        
        Args:
            papers: 論文情報のリスト
        """
        existing_urls = self.notion_client.get_existing_paper_urls()
        
        for paper in papers:
            paper_url = paper['pdf_url']
            
            # 既に登録済みの論文はスキップ
            if paper_url in existing_urls:
                print(f"Skipping existing paper: {paper['title']}")
                continue
            
            # GitHubのURLを抽出（単一のURL）
            github_url = self.extract_github_url(paper['summary'], paper_url)
            
            # Notionに登録する論文情報を作成
            notion_paper_info = {
                'title': paper['title'],
                'paper_url': paper_url,
                'github_url': github_url,  # 単一のURLとして設定
                'published': paper['published'],
                'keywords': paper['keywords']
            }
            
            try:
                self.notion_client.create_page(notion_paper_info)
                print(f"Added paper to Notion: {paper['title']}")
            except Exception as e:
                print(f"Error adding paper to Notion: {paper['title']}, Error: {e}")

def main():
    # 環境変数から設定を取得
    notion_token = os.getenv("NOTION_TOKEN")
    notion_database_id = os.getenv("NOTION_DATABASE_ID")
    keywords_str = input("Enter keywords separated by commas: ")
    
    if not notion_token or not notion_database_id:
        raise ValueError("NOTION_TOKEN and NOTION_DATABASE_ID environment variables are required")
    
    # キーワードのリストを作成
    keywords = [k.strip() for k in keywords_str.split(",")]
    print(f"Filtering papers with keywords: {keywords}")
    
    # NotionクライアントとArxivPaperProcessorを初期化
    notion_client = NotionClient(notion_token, notion_database_id)
    paper_processor = ArxivPaperProcessor(notion_client)
    
    # arxivから論文を取得（キーワードでフィルタリング）
    fetcher = ArxivFetcher(keywords=keywords, max_results=1000)
    papers = fetcher.fetch_papers()
    
    print(f"Found {len(papers)} papers matching the criteria")
    
    # 取得した論文を処理してNotionに登録
    paper_processor.process_papers(papers)

if __name__ == "__main__":
    main()
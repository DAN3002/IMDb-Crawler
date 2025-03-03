import json
import time
from datetime import datetime
import requests
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from utils.logger import setup_logger
import os
import html  # Add this import at the top of the file

class UserReviewCrawler:
    def __init__(self):
        # Setup logger
        log_file = f'./logs/user_review_crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        self.logger = setup_logger('UserReviewCrawler', log_file)
        
        self.PAGE_SIZE = 25
        self.base_url = "https://caching.graphql.imdb.com/"
        self.headers = {
            'accept': 'application/graphql+json, application/json',
            'accept-language': 'vi-VN,vi;q=0.9,en-GB;q=0.8,en;q=0.7,fr-FR;q=0.6,fr;q=0.5,en-US;q=0.4',
            'content-type': 'application/json',
            'origin': 'https://www.imdb.com',
            'referer': 'https://www.imdb.com/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            'x-imdb-client-name': 'imdb-web-next',
            'x-imdb-user-country': 'VN',
            'x-imdb-user-language': 'vi-VN'
        }
        
        self.output_folder = './output'
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            self.logger.info(f"Created output directory: {self.output_folder}")
            
        self.session = requests.Session()
        self._init_session()

    def _init_session(self):
        """Initialize session with browser automation"""
        try:
            self.logger.info("Setting up Chrome for session initialization...")
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            
            driver = webdriver.Chrome(options=chrome_options)
            self.logger.info("Chrome driver initialized")
            
            driver.get('https://www.imdb.com/')
            self.logger.info("Visiting IMDb homepage")
            
            time.sleep(5)
            
            cookies = driver.get_cookies()
            self.logger.info(f"Retrieved {len(cookies)} cookies")
            
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
            
            driver.quit()
            self.logger.info("Session initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing session: {str(e)}")

    def get_movie_reviews(self, movie_id, movie_name, original_title):
        """Get all reviews for a specific movie"""
        after_token = ""
        has_next = True
        all_reviews = []
        page = 1
        
        self.logger.info(f"Starting to fetch reviews for movie {movie_name} ({movie_id})")
        
        while has_next:
            try:
                self.logger.info(f"Fetching page {page} with after_token: {after_token}")
                reviews_page = self._fetch_reviews_page(movie_id, after_token)
                
                if not reviews_page or 'data' not in reviews_page:
                    self.logger.error(f"Failed to fetch page {page}")
                    break
                
                # Extract reviews data
                reviews_data = reviews_page['data']['title']['reviews']
                edges = reviews_data.get('edges', [])
                
                # Process reviews from current page
                for edge in edges:
                    review = self._extract_review_data(edge, movie_id, movie_name, original_title)
                    if review:
                        all_reviews.append(review)
                
                self.logger.info(f"Processed {len(edges)} reviews on page {page}")
                
                # Update pagination info
                page_info = reviews_data.get('pageInfo', {})
                has_next = page_info.get('hasNextPage', False)
                after_token = page_info.get('endCursor', '')
                
                if not has_next:
                    self.logger.info("Reached last page")
                    break
                
                page += 1
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                self.logger.error(f"Error processing page {page}: {str(e)}")
                break
        
        return all_reviews

    def _fetch_reviews_page(self, movie_id, after_token=""):
        variables = {
            "after": after_token,
            "const": movie_id,
            "filter": {},
            "first": self.PAGE_SIZE,
            "locale": "vi-VN",
            "sort": {
                "by": "HELPFULNESS_SCORE",
                "order": "DESC"
            }
        }
        
        extensions = {
            "persistedQuery": {
                "sha256Hash": "89aff4cd7503e060ff1dd5aba91885d8bac0f7a21aa1e1f781848a786a5bdc19",
                "version": 1
            }
        }
        
        encoded_variables = quote(json.dumps(variables))
        encoded_extensions = quote(json.dumps(extensions))
        
        url = f"{self.base_url}?operationName=TitleReviewsRefine&variables={encoded_variables}&extensions={encoded_extensions}"
        
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"API request failed with status code {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error making API request: {str(e)}")
            return None

    def _extract_review_data(self, edge, movie_id, movie_name, original_title):
        try:
            node = edge.get('node', {})
            if not node:
                return None
                
            # Get the raw content
            raw_content = node.get('text', {}).get('originalText', {}).get('plaidHtml', '')
            raw_title = node.get('summary', {}).get('originalText', '')
            
            # Clean up the content by:
            # 1. Decode HTML entities (&#39; -> ', &quot; -> ", etc)
            # 2. Replace <br/> with newlines
            clean_content = html.unescape(raw_content)
            clean_content = clean_content.replace('<br/>', '\n').replace('<br>', '\n')
            
            # Clean up the title
            clean_title = html.unescape(raw_title)
            
            # Extract review data
            review = {
                'review_id': node.get('id', ''),
                'movie_id': movie_id,
                'movie_name': movie_name,
                'original_title': original_title,
                'review_title': clean_title,
                'review_content': clean_content,
                'spoiler': node.get('spoiler', False),
                'rating': node.get('authorRating'),
                'like': node.get('helpfulness', {}).get('upVotes', 0),
                'dislike': node.get('helpfulness', {}).get('downVotes', 0),
                'reviewer_username': node.get('author', {}).get('nickName', ''),
                'submission_date': node.get('submissionDate', ''),
                'updated_at': datetime.now().isoformat()
            }
            
            return review
            
        except Exception as e:
            self.logger.error(f"Error extracting review data: {str(e)}")
            return None

    def crawl_movies_reviews(self, input_file, output_file):
        """Crawl reviews for all movies in the input file"""
        try:
            # Read movies from input file
            with open(input_file, 'r', encoding='utf-8') as f:
                movies = json.load(f)
            
            all_reviews = []
            
            # Process each movie
            for movie in movies:
                movie_id = movie['id']
                movie_name = movie['name']
                original_title = movie['original_title']
                
                self.logger.info(f"\nProcessing movie: {movie_name} ({movie_id})")
                
                # Get reviews for this movie
                movie_reviews = self.get_movie_reviews(movie_id, movie_name, original_title)
                all_reviews.extend(movie_reviews)
                
                self.logger.info(f"Found {len(movie_reviews)} reviews for {movie_name}")
                
                # Save progress after each movie
                self._save_reviews(all_reviews, output_file)
                
                time.sleep(3)  # Rate limiting between movies
            
            return all_reviews
            
        except Exception as e:
            self.logger.error(f"Error in crawl_movies_reviews: {str(e)}")
            return []

    def _save_reviews(self, reviews, output_file):
        """Save reviews to output file"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(reviews, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Saved {len(reviews)} reviews to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving reviews: {str(e)}")

def main():
    # Create required directories
    for directory in ['./logs', './output']:
        if not os.path.exists(directory):
            os.makedirs(directory)
    
    crawler = UserReviewCrawler()
    
    # Input and output files
    input_file = "output/filtered_movies.json"  # Use filtered movies
    output_file = "output/movie_reviews.json"
    
    # Crawl reviews
    reviews = crawler.crawl_movies_reviews(input_file, output_file)
    
    crawler.logger.info(f"\nCrawling completed. Total reviews collected: {len(reviews)}")

if __name__ == "__main__":
    main() 
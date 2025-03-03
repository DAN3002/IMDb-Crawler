import pandas as pd
from imdb import Cinemagoer
import requests
import time
from datetime import datetime
import json
import os
from urllib.parse import quote, unquote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

class IMDbCrawler:
    def __init__(self):
        self.base_url = "https://caching.graphql.imdb.com/"
        self.headers = {
            'authority': 'caching.graphql.imdb.com',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://www.imdb.com',
            'referer': 'https://www.imdb.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            # Add required cookies
            'cookie': 'session-id=xxx; session-id-time=xxx; ubid-main=xxx',
        }
        self.output_folder = 'imdb_data'
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        self.all_movies = []
        # Initialize session to maintain cookies
        self.session = requests.Session()
        self._init_session()

    def _init_session(self):
        """Initialize session with browser automation"""
        try:
            # Setup Chrome options
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Run in headless mode
            
            # Initialize browser
            driver = webdriver.Chrome(options=chrome_options)
            
            # Visit IMDb
            driver.get('https://www.imdb.com/')
            
            # Wait for cookies to be set
            time.sleep(5)
            
            # Get cookies from browser
            cookies = driver.get_cookies()
            
            # Add cookies to session
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
            
            # Update headers
            self.headers['cookie'] = '; '.join([
                f'{cookie["name"]}={cookie["value"]}'
                for cookie in cookies
            ])
            
            # Close browser
            driver.quit()
            
        except Exception as e:
            print(f"Error initializing session: {str(e)}")

    def get_vietnamese_movies(self):
        after_token = None  # Start with no token for first page
        has_next = True
        
        while has_next:
            try:
                print(f"\nFetching page with after_token: {after_token}")
                movies_page = self._fetch_movies_page(after_token)
                
                if not movies_page:
                    print("Failed to fetch page, retrying...")
                    time.sleep(5)
                    continue
                
                # Extract data from the response
                search_results = movies_page.get('data', {}).get('advancedTitleSearch', {})
                edges = search_results.get('edges', [])
                page_info = search_results.get('pageInfo', {})
                
                # Process movies from current page
                valid_movies = 0
                for edge in edges:
                    movie_data = self._extract_movie_data(edge)
                    if movie_data:
                        self.all_movies.append(movie_data)
                        valid_movies += 1
                        print(f"Found movie: {movie_data['title']} ({movie_data['id']})")
                
                print(f"Successfully processed {valid_movies} out of {len(edges)} movies on this page")
                
                # Update pagination info
                has_next = page_info.get('hasNextPage', False)
                after_token = page_info.get('endCursor')
                
                print(f"Total movies collected: {len(self.all_movies)}")
                
                # Save progress after each page
                self._save_progress()
                
                # Rate limiting
                time.sleep(2)
                
            except Exception as e:
                print(f"Error processing page: {str(e)}")
                time.sleep(5)
                continue
        
        return self.all_movies

    def _fetch_movies_page(self, after_token=None):
        # Base variables
        variables = {
            "first": 100,
            "locale": "vi-VN",
            "originCountryConstraint": {
                "anyPrimaryCountries": ["VN"]
            },
            "sortBy": "POPULARITY",
            "sortOrder": "ASC"
        }
        
        # Add after token for pagination if provided
        if after_token:
            variables["after"] = after_token
        
        # URL encode the variables
        encoded_variables = quote(json.dumps(variables))
        
        # URL encode the extensions
        extensions = {
            "persistedQuery": {
                "sha256Hash": "6842af47c3f1c43431ae23d394f3aa05ab840146b146a2666d4aa0dc346dc482",
                "version": 1
            }
        }
        encoded_extensions = quote(json.dumps(extensions))
        
        # Construct the full URL with properly encoded parameters
        url = f"{self.base_url}?operationName=AdvancedTitleSearch&variables={encoded_variables}&extensions={encoded_extensions}" 
        # print(f"Requesting URL: {url}")
        
        try:
            response = self.session.get(
                url,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract pagination info
                if data and 'data' in data:
                    search_results = data['data'].get('advancedTitleSearch', {})
                    page_info = search_results.get('pageInfo', {})
                    
                    # Print pagination details for debugging
                    print(f"Has next page: {page_info.get('hasNextPage', False)}")
                    print(f"End cursor: {page_info.get('endCursor', None)}")
                    
                    return data
            else:
                print(f"Error: API request failed with status code {response.status_code}")
                print(response.text)
                
                if "challenge-container" in response.text:
                    print("Detected challenge page, reinitializing session...")
                    self._init_session()
                    time.sleep(5)
            
            return None
            
        except Exception as e:
            print(f"Error making API request: {str(e)}")
            return None

    def _extract_movie_data(self, movie_edge):
        try:
            if not movie_edge or 'node' not in movie_edge:
                print("Invalid movie edge data")
                return None
            
            node = movie_edge['node']
            if not node:
                print("Empty node data")
                return None
            
            title = node.get('title', {})
            if not title:
                print(f"No title data found in node: {node}")
                return None

            # Extract data with safe fallbacks
            title_text = title.get('titleText', {})
            release_year = title.get('releaseYear', {})
            ratings_summary = title.get('ratingsSummary', {})
            runtime = title.get('runtime', {})
            genres_data = title.get('genres', {})
            plot_data = title.get('plot', {})
            
            # Build movie data with careful extraction
            movie_data = {
                'id': title.get('id', ''),
                'title': title_text.get('text', '') if title_text else '',
                'year': release_year.get('year', '') if release_year else '',
                'rating': ratings_summary.get('aggregateRating') if ratings_summary else None,
                'votes': ratings_summary.get('voteCount', 0) if ratings_summary else 0,
                'runtime_minutes': runtime.get('seconds', 0) // 60 if runtime and runtime.get('seconds') else 0,
                'genres': [],
                'plot': '',
                'primary_image': title.get('primaryImage', {}).get('url', '')
            }
            
            # Safely extract genres
            if genres_data and 'genres' in genres_data:
                movie_data['genres'] = [
                    genre.get('text', '') 
                    for genre in genres_data['genres'] 
                    if genre and isinstance(genre, dict)
                ]
            
            # Safely extract plot
            if plot_data:
                plot_text = plot_data.get('plotText', {})
                if plot_text:
                    movie_data['plot'] = plot_text.get('plainText', '')
            
            # Debug print for empty required fields
            if not movie_data['id'] or not movie_data['title']:
                print(f"Warning: Missing required data - ID: {movie_data['id']}, Title: {movie_data['title']}")
                print(f"Original node data: {json.dumps(node, indent=2)}")
                return None
            
            return movie_data
            
        except Exception as e:
            print(f"Error extracting movie data: {str(e)}")
            print(f"Problematic movie_edge: {json.dumps(movie_edge, indent=2)}")
            return None

    def _save_progress(self):
        """Save the current progress to a file"""
        try:
            with open('./output/vietnamese_movies_progress.json', 'w', encoding='utf-8') as f:
                json.dump(self.all_movies, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error saving progress: {str(e)}")

def main():
    crawler = IMDbCrawler()
    movies = crawler.get_vietnamese_movies()
    
    # Save final results
    with open('./output/vietnamese_movies.json', 'w', encoding='utf-8') as f:
        json.dump(movies, f, ensure_ascii=False, indent=2)
    
    print(f"\nCrawling completed. Total movies found: {len(movies)}")

if __name__ == "__main__":
    main() 
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
from utils.logger import setup_logger

class IMDbCrawler:
	def __init__(self):
		# Setup logger
		log_file = f'./logs/imdb_crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
		self.logger = setup_logger('IMDbCrawler', log_file)
		
		self.PAGE_SIZE = 100
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
		self.output_folder = './output'
		if not os.path.exists(self.output_folder):
			os.makedirs(self.output_folder)
			self.logger.info(f"Created output directory: {self.output_folder}")
		self.all_movies = []
		self.error_count = 0  # Add error counter
		# Initialize session to maintain cookies
		self.logger.info("Initializing session...")
		self.session = requests.Session()
		self._init_session()

	def _init_session(self):
		"""Initialize session with browser automation"""
		try:
			self.logger.info("Setting up Chrome for session initialization...")
			chrome_options = Options()
			chrome_options.add_argument('--headless')  # Run in headless mode
			
			# Initialize browser
			driver = webdriver.Chrome(options=chrome_options)
			self.logger.info("Chrome driver initialized")
			
			# Visit IMDb
			driver.get('https://www.imdb.com/')
			self.logger.info("Visiting IMDb homepage")
			
			# Wait for cookies to be set
			time.sleep(5)
			
			# Get cookies from browser
			cookies = driver.get_cookies()
			self.logger.info(f"Retrieved {len(cookies)} cookies")
			
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
			self.logger.info("Session initialization completed successfully")
			
		except Exception as e:
			self.logger.error(f"Error initializing session: {str(e)}")

	def get_vietnamese_movies(self):
		after_token = None
		has_next = True
		page_errors = 0  # Track page-level errors
		max_retries = 3  # Maximum number of retries per page
		
		self.logger.info("Starting to fetch Vietnamese movies...")
		
		while has_next:
			try:
				self.logger.info(f"Fetching page with after_token: {after_token}")
				movies_page = self._fetch_movies_page(after_token)
				
				if not movies_page:
					page_errors += 1
					if page_errors >= max_retries:
						self.logger.error(f"Failed to fetch page after {max_retries} attempts. Stopping crawl.")
						break
					self.logger.warning(f"Failed to fetch page, retry {page_errors}/{max_retries}...")
					time.sleep(5)
					continue
				
				# Reset page errors on successful fetch
				page_errors = 0
				
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
						self.logger.info(f"Found movie: {movie_data['title']} ({movie_data['id']})")
				
				self.logger.info(f"Successfully processed {valid_movies} out of {len(edges)} movies on this page")
				self.logger.info(f"Total movies collected: {len(self.all_movies)}")
				
				# Update pagination info - explicitly check hasNextPage
				has_next = bool(page_info.get('hasNextPage', False))
				if not has_next:
					self.logger.info("Reached last page. Stopping crawl.")
					break
				
				after_token = page_info.get('endCursor')
				if not after_token:
					self.logger.warning("No endCursor found for next page. Stopping crawl.")
					break
				
				# Save progress after each page
				self._save_progress()
				
				# Rate limiting
				time.sleep(2)
				
			except Exception as e:
				page_errors += 1
				if page_errors >= max_retries:
					self.logger.error(f"Too many errors ({page_errors}). Stopping crawl.")
					break
				self.logger.error(f"Error processing page: {str(e)}")
				time.sleep(5)
				continue
		
		# Print error statistics at the end
		self.logger.info("\nCrawling Statistics:")
		self.logger.info(f"Total page errors: {page_errors}")
		self.logger.info(f"Total movie processing errors: {self.error_count}")
		return self.all_movies

	def _fetch_movies_page(self, after_token=None):
		# Base variables
		variables = {
			"first": self.PAGE_SIZE,
			"locale": "vi-VN",
			"originCountryConstraint": {
				"anyPrimaryCountries": ["VN"]
			},
			"titleTypeConstraint":{"anyTitleTypeIds":["movie"],"excludeTitleTypeIds":[]},
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
					self.logger.debug(f"Has next page: {page_info.get('hasNextPage', False)}")
					self.logger.debug(f"End cursor: {page_info.get('endCursor', None)}")
					
					return data
			else:
				self.logger.error(f"Error: API request failed with status code {response.status_code}")
				self.logger.error(response.text)
				
				if "challenge-container" in response.text:
					self.logger.warning("Detected challenge page, reinitializing session...")
					self._init_session()
					time.sleep(5)
			
			return None
			
		except Exception as e:
			self.logger.error(f"Error making API request: {str(e)}")
			return None

	def _extract_movie_data(self, movie_edge):
		try:
			if not movie_edge or 'node' not in movie_edge:
				self.error_count += 1
				self.logger.warning("Invalid movie edge data")
				return None
			
			node = movie_edge['node']
			if not node:
				self.error_count += 1
				self.logger.warning("Empty node data")
				return None
			
			title = node.get('title', {})
			if not title:
				self.error_count += 1
				self.logger.warning("No title data found in node")
				return None

			# Extract data with safe fallbacks
			title_text = title.get('titleText', {})
			release_year = title.get('releaseYear', {})
			ratings_summary = title.get('ratingsSummary', {})
			runtime = title.get('runtime', {})
			genres_data = title.get('titleGenres', {})
			plot_data = title.get('plot', {})
			primary_image = title.get('primaryImage')
			
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
				'primary_image': primary_image.get('url', '') if primary_image else ''
			}
			
			# Safely extract genres with new format
			if genres_data and 'genres' in genres_data:
				movie_data['genres'] = [
					genre.get('genre', {}).get('text', '')
					for genre in genres_data['genres']
					if genre and isinstance(genre, dict) and 'genre' in genre
				]
			
			# Safely extract plot
			if plot_data:
				plot_text = plot_data.get('plotText', {})
				if plot_text:
					movie_data['plot'] = plot_text.get('plainText', '')
			
			# Check for missing required fields without printing
			if not movie_data['id'] or not movie_data['title']:
				self.error_count += 1
				self.logger.warning(f"Missing required data for movie {movie_data.get('id', 'unknown')}")
				return None
			
			return movie_data
			
		except Exception as e:
			self.error_count += 1
			self.logger.error(f"Error extracting movie data: {str(e)}")
			return None

	def _save_progress(self):
		"""Save the current progress to a file"""
		try:
			progress_file = f'{self.output_folder}/vietnamese_movies_progress.json'
			with open(progress_file, 'w', encoding='utf-8') as f:
				json.dump(self.all_movies, f, ensure_ascii=False, indent=4)
			self.logger.info(f"Progress saved to {progress_file}")
		except Exception as e:
			self.logger.error(f"Error saving progress: {str(e)}")

def main():
	# Create required directories
	for directory in ['./error_logs', './output', './logs']:
		if not os.path.exists(directory):
			os.makedirs(directory)
			
	crawler = IMDbCrawler()
	movies = crawler.get_vietnamese_movies()
	
	# Save final results
	output_file = './output/vietnamese_movies.json'
	with open(output_file, 'w', encoding='utf-8') as f:
		json.dump(movies, f, ensure_ascii=False, indent=4)
	
	crawler.logger.info(f"Crawling completed. Total movies found: {len(movies)}")

if __name__ == "__main__":
	main() 
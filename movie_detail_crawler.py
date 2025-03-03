import json
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from utils.logger import setup_logger

class MovieDetailCrawler:
	def __init__(self):
		# Setup logger
		log_file = f'./logs/movie_detail_crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
		self.logger = setup_logger('MovieDetailCrawler', log_file)
		
		# Setup Chrome options
		self.chrome_options = Options()
		# self.chrome_options.add_argument('--headless')  # Run in headless mode
		self.chrome_options.add_argument('--disable-gpu')
		self.chrome_options.add_argument('--no-sandbox')
		self.chrome_options.add_argument('--disable-dev-shm-usage')
		
		self.logger.info("Initializing Chrome driver...")
		self.driver = webdriver.Chrome(options=self.chrome_options)
		self.wait = WebDriverWait(self.driver, 10)
		self.logger.info("Chrome driver initialized successfully")

	def get_movie_details(self, movie_id, original_data):
		try:
			# Construct IMDb movie URL
			url = f"https://www.imdb.com/title/{movie_id}/"
			self.logger.info(f"Fetching details for movie {movie_id} from {url}")
			self.driver.get(url)
			
			# Wait for and find the NEXT_DATA script tag
			script_element = self.wait.until(
				EC.presence_of_element_located((By.ID, '__NEXT_DATA__'))
			)
			
			# Extract and parse JSON data
			json_data = json.loads(script_element.get_attribute('innerHTML'))
			
			# Safely get aboveTheFoldData
			page_props = json_data.get('props', {}).get('pageProps', {})
			if not page_props:
				print(f"No pageProps found for movie {movie_id}")
				return None
			
			# Get both data sources
			above_fold_data = page_props.get('aboveTheFoldData')
			main_column_data = page_props.get('mainColumnData')
			
			if not above_fold_data or not main_column_data:
				print(f"Missing required data sections for movie {movie_id}")
				return None
			
			# Safely get nested data from aboveTheFoldData
			title_text = above_fold_data.get('titleText') or {}
			original_title_text = above_fold_data.get('originalTitleText') or {}
			release_year = above_fold_data.get('releaseYear') or {}
			ratings_summary = above_fold_data.get('ratingsSummary') or {}
			reviews = above_fold_data.get('reviews') or {}
			critic_reviews = above_fold_data.get('criticReviews') or {}
			certificate = above_fold_data.get('certificate') or {}
			meter_ranking = above_fold_data.get('meterRanking') or {}
			runtime = above_fold_data.get('runtime') or {}
			
			# Get countries from mainColumnData
			countries_data = main_column_data.get('countriesOfOrigin') or {}
			
			# Extract required information with safe fallbacks
			details = {
				'id': movie_id,
				'name': title_text.get('text', ''),
				'original_title': original_title_text.get('text', ''),
				'year': release_year.get('year', ''),
				'rating': ratings_summary.get('aggregateRating'),
				'votes': ratings_summary.get('voteCount', 0),
				'user_reviews_count': reviews.get('total', 0),
				'critic_reviews_count': critic_reviews.get('total', 0),
				'countries': [
					country.get('text', '')
					for country in countries_data.get('countries', [])
					if country and isinstance(country, dict)
				],
				'certificate': certificate.get('rating', ''),
				'popularity_rank': meter_ranking.get('currentRank'),
				'genres': original_data.get('genres', []),
				'runtime_minutes': runtime.get('seconds', 0) // 60 if runtime.get('seconds') else 0,
				'plot': original_data.get('plot', ''),
				'primary_image': original_data.get('primary_image', ''),
				'link': f"https://www.imdb.com/title/{movie_id}",
				'last_updated': datetime.now().isoformat()
			}
			
			# Update with any missing data from original data
			if not details['rating']:
				details['rating'] = original_data.get('rating')
			if not details['votes']:
				details['votes'] = original_data.get('votes')
			if not details['name']:
				details['name'] = original_data.get('title', '')
			if not details['original_title']:
				details['original_title'] = details['name']
			
			# Validate required fields
			if not details['id'] or not details['name']:
				print(f"Missing required data for movie {movie_id}")
				print(f"Current details: {json.dumps(details, indent=2)}")
				return None
			
			self.logger.info(f"Successfully extracted data for {details['name']} ({details['year']})")
			return details
			
		except Exception as e:
			self.logger.error(f"Error fetching details for movie {movie_id}: {str(e)}")
			# Save both error and data for debugging
			try:
				debug_data = {
					'error': str(e),
					'page_source': self.driver.page_source,
					'json_data': json_data if 'json_data' in locals() else None,
					'above_fold_data': above_fold_data if 'above_fold_data' in locals() else None
				}
				error_file = f'error_logs/error_{movie_id}_debug.json'
				with open(error_file, 'w', encoding='utf-8') as f:
					json.dump(debug_data, f, ensure_ascii=False, indent=4)
				self.logger.info(f"Error details saved to {error_file}")
			except Exception as debug_error:
				self.logger.error(f"Failed to save debug data: {str(debug_error)}")
			return None

	def process_movies_file(self, input_file='./output/vietnamese_movies.json', 
						  output_file='./output/movie_details.json'):
		try:
			self.logger.info(f"Starting to process movies from {input_file}")
			# Read input file
			with open(input_file, 'r', encoding='utf-8') as f:
				movies = json.load(f)
			
			detailed_movies = []
			total_movies = len(movies)
			self.logger.info(f"Found {total_movies} movies to process")
			
			print(f"Processing {total_movies} movies...")
			
			for idx, movie in enumerate(movies, 1):
				movie_id = movie.get('id')
				if not movie_id:
					self.logger.warning(f"Skipping movie at index {idx}: No movie ID found")
					continue
				
				self.logger.info(f"Processing movie {idx}/{total_movies}: {movie_id}")
				
				details = self.get_movie_details(movie_id, movie)
				if details:
					detailed_movies.append(details)
					self.logger.info(f"Successfully processed movie: {details['name']}")
				
				# Save progress periodically
				if idx % 5 == 0:
					self._save_progress(detailed_movies, output_file)
					self.logger.info(f"Progress saved: {len(detailed_movies)}/{idx} movies processed")
				
				# Rate limiting
				time.sleep(2)
			
			# Save final results
			self._save_progress(detailed_movies, output_file)
			
			self.logger.info(f"Processing completed. Found {len(detailed_movies)} movies.")
			return detailed_movies
			
		except Exception as e:
			self.logger.error(f"Error processing movies file: {str(e)}")
			return []
		finally:
			# Clean up
			self.driver.quit()
			self.logger.info("Chrome driver closed")

	def _save_progress(self, movies, output_file):
		"""Save current progress to file"""
		try:
			with open(output_file, 'w', encoding='utf-8') as f:
				json.dump(movies, f, ensure_ascii=False, indent=4)
			self.logger.info(f"Progress saved to {output_file}")
		except Exception as e:
			self.logger.error(f"Error saving progress: {str(e)}")

	def _extract_movie_data(self, movie):
		try:
			# Get existing movie data
			movie_data = {
				'id': movie['id'],
				'name': movie['name'],
				'original_title': movie['original_title'],
				'year': movie['year'],
				'rating': movie['rating'],
				'votes': movie['votes'],
				'user_reviews_count': movie['user_reviews_count'],
				'critic_reviews_count': movie['critic_reviews_count'],
				'countries': movie['countries'],
				'certificate': movie['certificate'],
				'popularity_rank': movie['popularity_rank'],
				'genres': movie['genres'],
				'runtime_minutes': movie['runtime_minutes'],
				'plot': movie['plot'],
				'primary_image': movie['primary_image'],
				'link': f"https://www.imdb.com/title/{movie['id']}",
				'last_updated': datetime.now().isoformat()
			}
			return movie_data
		except Exception as e:
			self.logger.error(f"Error extracting movie data: {str(e)}")
			return None

def main():
	# Create required directories
	for directory in ['./error_logs', './output', './logs']:
		if not os.path.exists(directory):
			os.makedirs(directory)
  
	crawler = MovieDetailCrawler()
	try:
		crawler.process_movies_file()
	except Exception as e:
		crawler.logger.error(f"Main process error: {str(e)}")
	finally:
		if hasattr(crawler, 'driver'):
			crawler.driver.quit()

if __name__ == "__main__":
	main() 
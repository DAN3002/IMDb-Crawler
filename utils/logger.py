import logging
import os
from datetime import datetime

def setup_logger(name, log_file):
	"""Set up logger with file and console handlers"""
	# Create logs directory if it doesn't exist
	log_dir = os.path.dirname(log_file)
	if not os.path.exists(log_dir):
		os.makedirs(log_dir)
	
	# Create logger
	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)
	
	# Create handlers
	file_handler = logging.FileHandler(log_file, encoding='utf-8')
	console_handler = logging.StreamHandler()
	
	# Create formatters and add it to handlers
	log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	file_handler.setFormatter(log_format)
	console_handler.setFormatter(log_format)
	
	# Add handlers to the logger
	logger.addHandler(file_handler)
	logger.addHandler(console_handler)
	
	return logger 
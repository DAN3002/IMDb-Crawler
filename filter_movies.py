import json

def filter_movies(input_file, output_file):
    # Read the JSON file
    with open(input_file, 'r', encoding='utf-8') as f:
        movies = json.load(f)

    # Filter movies based on criteria:
    # - votes > 100
    # - country is only Vietnam
    # - user_reviews_count >= 5
    filtered_movies = [
        movie for movie in movies
        if movie['votes'] > 100
        # if True 
        and movie['countries'] == ['Vietnam']
        and movie['user_reviews_count'] >= 5
    ]

    # Save filtered movies to new file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_movies, f, indent=4, ensure_ascii=False)

    print(f"Filtered {len(filtered_movies)} movies out of {len(movies)} total movies")
    return filtered_movies

if __name__ == "__main__":
    input_file = "output/movie_details.json"
    output_file = "output/filtered_movies.json"
    filtered_movies = filter_movies(input_file, output_file) 
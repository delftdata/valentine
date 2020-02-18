from collections import defaultdict


range_syntactic_links = set([el for el in range(50)])  # does not include 50
range_semantic_links = set([el + 50 for el in range(50)])


def process_file(path, min_ratings_per_link=3):
    # Read data into structures
    with open(path, 'r') as f:
        ratings_per_link = defaultdict(list)
        for line in f.readlines():
            tokens = line.split(',')
            lid = tokens[0]
            rating = tokens[1]
            ratings_per_link[lid].append(rating)
    # Check we have at least min_ratings_per_link
    for k, v in ratings_per_link.items():
        if len(v) < min_ratings_per_link:
            print("Insufficient ratings for: " + str(k) + " - have: " + str(len(v)))
    # Aggregate them
    avg_rating_per_link = dict()
    for k, v in ratings_per_link.items():
        sum_rating = 0
        for rating in v:
            sum_rating += rating
        avg_rating = sum_rating / len(v)
        avg_rating_per_link[k] = avg_rating
    # Aggregate them per class
    syntactic_class_avg = 0
    semantic_class_avg = 0
    for k, v in avg_rating_per_link.items():
        if k in range_syntactic_links:
            syntactic_class_avg += v
        elif k in range_semantic_links:
            semantic_class_avg += v
    syntactic_class_avg = syntactic_class_avg / len(range_syntactic_links)
    semantic_class_avg = semantic_class_avg / len(range_semantic_links)
    return avg_rating_per_link, syntactic_class_avg, semantic_class_avg


if __name__ == "__main__":
    print("Result processor")

    avg_rating_per_link, avg_syn, avg_sem = process_file("file_path")

    print("Avg syn: " + str(avg_syn))
    print("Avg sem: " + str(avg_sem))

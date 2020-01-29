from similarity_flooding import SimilarityFlooding
from utils import parseFile

if __name__ == "__main__":

    G1 = parseFile("personnel", "./Datasets/personnel.txt")
    G2 = parseFile("empdep", "./Datasets/empdep.txt")

    SF = SimilarityFlooding(G1,G2, 'inverse_average', 'formula_c')

    SF.calculate_initial_mapping()

    matches = SF.fixpoint_computation(100, 0.001)

    filtered_matches = SF.filter_map(matches)

    SF.print_results(filtered_matches)

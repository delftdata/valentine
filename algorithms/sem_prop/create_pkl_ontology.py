import sys


def main(argv):
    if len(argv) < 2:
        print('create_pkl_ontology.py <inputfile> <outputfile>')
        sys.exit(2)

    input_owl = argv[0]
    output_pkl = argv[1]

    print('Input file is ', input_owl)
    print('Output file is ', output_pkl)

    from ontomatch import onto_parser as op

    op.parse_ontology(input_owl, output_pkl)


if __name__ == "__main__":
    main(sys.argv[1:])

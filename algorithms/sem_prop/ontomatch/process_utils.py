

with open("/Users/ra-mit/development/discovery_proto/ontomatch/links_massdata_dbpedia", "r") as f:
    lines = f.readlines()
    # lines = lines[:3]
    print("Total lines: " + str(len(lines)))
    isalines = []
    for l in lines:
        base = str(l).split(")")
        tokens = base[1].split(",")
        r = tokens[1].strip().replace("'", "")
        # print(r)
        if r == 'is_a':
            isalines.append(l)
    print("Filtered lines: " + str(len(isalines)))

    seen = set()
    clean_lines = []
    for l in isalines:
        l = l.replace("(", "")
        l = l.replace(")", "")
        tokens = l.split(",")
        table_a = tokens[1].strip().replace("'", "")
        table_b = tokens[5].strip().replace("'", "")

        if table_a + table_b not in seen or table_b + table_a not in seen:
            clean_line = table_a + "," + table_b
            clean_lines.append(clean_line)
        seen.add(table_a + table_b)
        seen.add(table_b + table_a)
    print("Total unique pairs: " + str(len(clean_lines)))

    with open("/Users/ra-mit/development/discovery_proto/ontomatch/links_massdata_clean", "w") as g:
        for cl in clean_lines:
            g.write(cl + '\n')
    print("Done")

from pickle import load, dump


def serialize_object(obj, path):
    f = open(path, 'wb')
    dump(obj, f)
    f.close()


def deserialize_object(path):
    f = open(path, 'rb')
    obj = load(f)
    return obj


if __name__ == "__main__":
    print("Input output")

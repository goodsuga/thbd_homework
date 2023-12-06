import yaml
from pprint import pprint
from pathlib import Path

from classes import CustomerBasket, UserClass
from binary_serializer import Serializer
import schema_pb2 as ProtobufSchema
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json


from time import time
from statistics import mean, stdev

from tqdm import tqdm

def run_my_serizlier(object, schema):
    ser = Serializer(schema, __name__)
    start = time()
    ser.serialize(object, Path("OUT/my_serializer.bin"))
    stop = time()
    save_time = round(stop-start, 5)
    start = time()
    ser.deserialize(Path("OUT/my_serializer.bin"))
    stop = time()
    load_time = round(stop-start, 5)
    return save_time, load_time


def run_protobuf_serializer(object):
    start = time()
    with open(Path("OUT/protobuf_serializer.pbf"), "wb") as fd:
        fd.write(object.SerializeToString())
    stop = time()
    save_time = round(stop-start, 5)

    start = time()
    _obj = object.__class__()
    with open(Path("OUT/protobuf_serializer.pbf"), "rb") as fd:
        _obj.ParseFromString(fd.read())
    stop = time()
    load_time = round(stop-start, 5)
    return save_time, load_time


def run_avro_serizlier(user, avro_schema):
    start = time()
    schema = avro.schema.parse(avro_schema)
    writer = DataFileWriter(open(Path("OUT/avro_user.avro"), "wb"), DatumWriter(), schema)
    avro_user = {
        "age": user.age,
        "name": user.name,
        "surname": user.surname,
        "current_balance": user.current_balance,
        "basket_items": user.basket.items,
        "basket_prices": user.basket.prices,
        "basket_quantities": user.basket.quantities,
        "basket_combos": user.basket.combos,
        "very_complicated_list_of_dicts_of_lists": user.very_complicated_list_of_dicts_of_lists
    }
    writer.append(avro_user)
    writer.close()
    stop = time()
    save_time = round(stop-start, 5)

    start = time()
    reader = DataFileReader(open(Path("OUT/avro_user.avro"), "rb"), DatumReader())
    for user in reader:
        pass
    
    reader.close()
    stop = time()
    load_time = round(stop-start, 5)
    return save_time, load_time
        


if __name__ == "__main__":

    N_RUN = 10000

    with open(Path("MySerializerData/example_schema.yaml"), "r") as r:
        schema = yaml.safe_load(r)

    user = UserClass()
    user.age = 27
    user.name = "John"
    user.surname = "Doe"
    user.current_balance = 100.0
    user.basket.items = ["Soap", "Latex Gloves", "Detergent"]
    user.basket.prices = [15.0, 15.0, 60.0]
    user.basket.quantities = [1, 1, 1]
    user.basket.combos = {
       "BigCheese": ["Cheese", "Sauce", "Napkins"],
       "CocaColaExtra": ["CocaCola", "Ice", "Bonus Glass"]
    }
    user.very_complicated_list_of_dicts_of_lists = [
        {"HelloWorld": [[1, 2], [3, 4], [5, 6]]},
        {
            "Hey, serializers!": [[10, 20, 30, 40, 50]],
            "Hey, yaml!": [[1100, 1200, 1300, 1400, 1500, 1600, 1700]]
        }
    ]
    print("Original user:")
    print(user)

    # ------------- МОЯ РЕАЛИЗАЦИЯ ----------
    # import cProfile

    # profile_fname = "run_test.prof"
    # profiler = cProfile.Profile()
    # result = profiler.runcall(run_my_serizlier, user, schema)
    # profiler.print_stats(sort='cumtime')
    # input(">>> ")

    my_save_times = []
    my_load_times = []
    for _ in tqdm(range(N_RUN), total=N_RUN):
        my_save_time, my_load_time = run_my_serizlier(user, schema)
        my_save_times.append(my_save_time)
        my_load_times.append(my_load_time)
    # -------------КОНЕЦ МОЕЙ РЕАЛИЗАЦИИ -----------

    # -------------------------------------------------------
    # PROTOBUF РЕАЛИЗАЦИЯ
    protobuf_user = ProtobufSchema.UserClass()
    protobuf_user.age = 27
    protobuf_user.name = "John"
    protobuf_user.surname = "Doe"
    protobuf_user.current_balance = 100.0
    protobuf_user.basket.items.extend(["Soap", "Latex Gloves", "Detergent"])
    protobuf_user.basket.prices.extend([15.0, 15.0, 60.0])
    protobuf_user.basket.quantities.extend([1, 1, 1])
    protobuf_user.basket.combos["BigCheese"].strings.extend(["Cheese", "Sauce", "Napkins"])
    protobuf_user.basket.combos["CocaColaExtra"].strings.extend(["CocaCola", "Ice", "Bonus Glass"])
    
    stlri1 = ProtobufSchema.StringToListOfRepeatedInts()
    ri = ProtobufSchema.RepeatedInt()
    ri.ints.extend([1, 2])
    stlri1.mapping["HelloWorld"].lists.append(ri)
    ri = ProtobufSchema.RepeatedInt()
    ri.ints.extend([3, 4])
    stlri1.mapping["HelloWorld"].lists.append(ri)
    ri = ProtobufSchema.RepeatedInt()
    ri.ints.extend([5, 6])
    stlri1.mapping["HelloWorld"].lists.append(ri)

    stlri2 = ProtobufSchema.StringToListOfRepeatedInts()
    ri = ProtobufSchema.RepeatedInt()
    ri.ints.extend([10, 20, 30, 40, 50])
    stlri2.mapping["Hey, serializers!"].lists.append(ri)
    ri = ProtobufSchema.RepeatedInt()
    ri.ints.extend([1100, 1200, 1300, 1400, 1500, 1600, 1700])
    stlri2.mapping["Hey, yaml!"].lists.append(ri)
    protobuf_user.very_complicated_list_of_dicts_of_lists.repeated_stlris.extend([stlri1, stlri2])

    protobuf_save_times = []
    protobuf_load_times = []
    for _ in tqdm(range(N_RUN), total=N_RUN):
        protobuf_save_time, protobuf_load_time = run_protobuf_serializer(protobuf_user)
        protobuf_save_times.append(protobuf_load_time)
        protobuf_load_times.append(protobuf_load_time)

    # КОНЕЦ PROTOBUF РЕАЛИЗАЦИИ
    # --------------------------------------------

    # ------------- FASTAVRO РЕАЛИЗАЦИЯ ----------
    avro_schema = {
        "namespace": "userclass.avro",
        "type": "record",
        "name": "UserClass",
        "fields": [
            {"name": "age", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "surname", "type": "string"},
            {"name": "current_balance", "type": "float"},
            {"name": "basket_items", "type": {"type": "array", "items": "string"}},
            {"name": "basket_prices", "type": {"type": "array", "items": "float"}},
            {"name": "basket_quantities", "type": {"type": "array", "items": "int"}},
            {"name": "basket_combos", "type": {
                "type": "map", "values": {
                    "type": "array", "items": "string"
                }
            }},
            {
                "name": "very_complicated_list_of_dicts_of_lists",
                "type": {
                    "type": "array",
                    "items": {
                            "type": "map",
                            "values": {
                                    "type": "array",
                                    "items": {
                                        "type": "array", "items": "int"
                                    }
                            }
                    }
                }
            }
        ]
    }

    # measure cpu time of run_avro_serizlier function
    avro_schema_json = json.dumps(avro_schema)
    avro_save_times = []
    avro_load_times = []
    for _ in tqdm(range(N_RUN), total=N_RUN):
        
        
        avro_save_time, avro_load_time = run_avro_serizlier(user, avro_schema_json)
        avro_save_times.append(avro_save_time)
        avro_load_times.append(avro_load_time)

    # -------------- КОНЕЦ FASTAVRO РЕАЛИЗАЦИИ -----

    import pandas as pd
    import os

    out = pd.DataFrame({
        "Method": [
            "My",
            "Protobuf",
            "Avro"
        ],
        "Total Save Time": [
            sum(my_save_times),
            sum(protobuf_save_times),
            sum(avro_save_times)
        ],
        "Std. of save time": [
            stdev(my_save_times),
            stdev(protobuf_save_times),
            stdev(avro_save_times)
        ],
        "Total Load Time": [
            sum(my_load_times),
            sum(protobuf_load_times),
            sum(avro_save_times)
        ],
        "Std. of load time": [
            stdev(my_load_times),
            stdev(protobuf_load_times),
            stdev(avro_save_times)
        ],
        "Out File Size": [
            os.path.getsize(Path("OUT/my_serializer.bin")),
            os.path.getsize(Path("OUT/protobuf_serializer.pbf")),
            os.path.getsize(Path("OUT/avro_user.avro")),
        ]
    })

    out = out.sort_values(by='Total Load Time').reset_index(drop=True)
    out.to_excel("Bench results.xlsx", index=False)

from typing import Dict, Union
from pathlib import Path
import struct
import inspect
import sys


class NullableItem:
    def __init__(self, item_type, item=None):
        self.item = item
        self.inner_type = item_type

    def __repr__(self) -> str:
        return f"Nullable {self.item.__class__.__name__} ({self.item.__repr__()})"


# class CodeBook:
#     def __init__(self, codes: Union[Dict, None] = None):
#         self.codes = {}
#         self.value_count = 0
#         if codes is not None:
#             self.codes.update(codes)
#             self.value_count = len(list(self.codes.keys()))

#     def __call__(self, key: str, size: int = 1):
#         self.codes[key] = int.to_bytes(self.value_count, size)
#         self.value_count += 1

#     def get_self):
#         return self.codes


class Serializer:
    def __init__(self, schema: Dict, caller_module_name):
        #caller = inspect.stack()[1]
        #caller_module = inspect.getmodule(caller[0])
        self.module = caller_module_name #caller_module.__name__

        self.schema = schema
        codes = [
            "class",
            "dict",
            "list",
            "float",
            "string",
            "int",
            "bool",
            "values",
            "keys",
            "classname",
            "value_type",
            "type",
            "object_data",

            # nullable types
            "nullable_string",
            "nullable_int",
            "nullable_float",
            "nullable_dict",
            "nullable_list",
            "nullable_class"
        ]

        self.codes = {k: int.to_bytes(v, 1) for v, k in enumerate(codes)}
        self.inverse_codes = {v: k for v, k in enumerate(self.codes.keys())}

        self.missing_value = int.to_bytes(255, 1)
        self.missing_value_int = 255
        


    def _sub_serialize(self, object, schema_to_use: Union[Dict, None] = None, n_tabs: int = 0):

        if schema_to_use is None:
            schema_to_use = self.schema
        
        if not isinstance(schema_to_use, dict):
            #print("\t"*n_tabs + f"{schema_to_use}: {object}")
            return object

        def subserialize_list():
            val_type = schema_to_use['value_type']
            return [
                self._sub_serialize(
                    sub_obj,
                    val_type,
                    n_tabs=n_tabs+1
                )
                for sub_obj in object
            ]
        
        def subserialize_dict():
            key_type = schema_to_use['value_type']['keys']
            values_type = schema_to_use['value_type']['values']
            res = {}
            for key, value in object.items():
                k = self._sub_serialize(
                    key,
                    key_type,
                    n_tabs=n_tabs+1
                )
                v = self._sub_serialize(
                    value,
                    values_type,
                    n_tabs=n_tabs+2
                )
                res[k] = v
            
            return res
        
        def subserialize_class():
            parse_results = {}
            for field in schema_to_use['value_type']:
                #print("\t"*n_tabs + f"{field}:")
                parse_results[field] = self._sub_serialize(
                    getattr(object, field),
                    schema_to_use['value_type'][field],
                    n_tabs=n_tabs+1
                )
            
            return parse_results

        field_type = schema_to_use['type']
        
        return {
            "list": subserialize_list,
            "dict": subserialize_dict,
            "class": subserialize_class,
            "nullable_class": lambda: NullableItem(dict, None if object is None else subserialize_class()),
            "nullable_list": lambda: NullableItem(list, None if object is None else subserialize_list()),
            "nullable_dict": lambda: NullableItem(dict, None if object is None else subserialize_dict()),
            "nullable_string": lambda: NullableItem(str, None if object is None else object),
            "nullable_int": lambda: NullableItem(int, None if object is None else object),
            "nullable_float": lambda: NullableItem(float, None if object is None else object)
        }[field_type]()


    def _recursive_dictionary_encode(self, item_to_code, n_tabs: int = 0) -> bytearray:
        #start = "\t"*n_tabs + f"{item_to_code}: "
        bytes_out = bytearray()

        def bool_to_bytes(is_nullable=False):
            if is_nullable:
                is_item_none = item_to_code.item is None
                if is_item_none:
                    return self.codes['nullable_bool'] + int(is_item_none).to_bytes(1)
                else:
                    return self.codes['nullable_bool'] + int(is_item_none).to_bytes(1) + int(item_to_code.item).to_bytes(1)
            # if not nullable
            return self.codes['bool'] + int(item_to_code).to_bytes(1)
        
        def int_to_bytes(is_nullable=False):
            if is_nullable:
                is_item_none = item_to_code.item is None
                if is_item_none:
                    return self.codes['nullable_int'] + int(is_item_none).to_bytes(1)
                else:
                    return self.codes['nullable_int'] + int(is_item_none).to_bytes(1) + int.to_bytes(item_to_code.item, 4)
            # if not nullable
            return self.codes['int'] + int.to_bytes(item_to_code, 4)
        
        def float_to_bytes(is_nullable=False):
            if is_nullable:
                is_item_none = item_to_code.item is None
                if is_item_none:
                    return self.codes['nullable_float'] + int(is_item_none).to_bytes(1)
                else:
                    return self.codes['nullable_float'] + int(is_item_none).to_bytes(1) + bytearray(struct.pack("d", item_to_code.item))
            # if not nullable
            return self.codes['float'] + bytearray(struct.pack("d", item_to_code))
        
        def string_to_bytes(is_nullable=False):
            bytes_to_add = self.codes.get(item_to_code.item if is_nullable else item_to_code, self.missing_value)
            bytes_out.extend(self.codes['nullable_string' if is_nullable else 'string'])

            if is_nullable:
                is_item_none = item_to_code.item is None
                bytes_out.extend(int(is_item_none).to_bytes(1))
                if is_item_none:
                    return bytes_out
            
            bytes_out.extend(bytes_to_add)

            if all([b1 == b2 for b1, b2 in zip(bytes_to_add, self.missing_value)]):
                # Имя поля не найдено
                # Следующие два байта укажут на длинну имени поля,
                encoded_string = item_to_code.item.encode('utf-8') if is_nullable else item_to_code.encode('utf-8')
                len_encoded_string = len(encoded_string)
                bytes_out.extend(len_encoded_string.to_bytes(4))
                # теперь само имя поля
                bytes_out.extend(encoded_string)
            
            return bytes_out
            
        
        def list_to_bytes(is_nullable=False):
            bytes_out.extend(self.codes['nullable_list' if is_nullable else 'list'])
            if is_nullable:
                is_item_none = item_to_code.item is None
                bytes_out.extend(int(is_item_none).to_bytes(1))
                if is_item_none:
                    return bytes_out
            
            # Иначе собираем список
            list_bytes = bytearray()
            
            for item in (item_to_code.item if is_nullable else item_to_code):
                item_bytes = self._recursive_dictionary_encode(
                    item,
                    n_tabs=n_tabs+1
                )
                list_bytes.extend(
                    int.to_bytes(len(item_bytes), 4)
                )
                list_bytes.extend(item_bytes)
            
            bytes_out.extend(int.to_bytes(
                len(list_bytes),
                4
            ))
            bytes_out.extend(list_bytes)
            return bytes_out
        
        def dict_to_bytes(is_nullable=False):
            bytes_out.extend(self.codes['nullable_dict' if is_nullable else 'dict'])
            if is_nullable:
                is_item_none = item_to_code.item is None
                bytes_out.extend(int(is_item_none).to_bytes(1))
                if is_item_none:
                    return bytes_out

            bytes_dict = bytearray()
            for key, value in (item_to_code.item if is_nullable else item_to_code).items():
                key_coded = self._recursive_dictionary_encode(key, n_tabs=n_tabs+1)
                value_coded = self._recursive_dictionary_encode(value, n_tabs=n_tabs+1)
                bytes_dict.extend(len(key_coded).to_bytes(4))
                bytes_dict.extend(key_coded)
                bytes_dict.extend(len(value_coded).to_bytes(4))
                bytes_dict.extend(value_coded)
            
            
            bytes_out.extend(len(bytes_dict).to_bytes(4))
            bytes_out.extend(bytes_dict)
            #print(f"{start}: {bytes_out} as dict")
            return bytes_out

        # Очень важно тут разместить bool перед int
        # т.к. isinstance(False, int) -> True
        # но isinstance(1, bool) -> False
        # т.е. булевы значения всегда будут считаться числами
        # но числа никогда не будут считаться булами.
        # поэтому если на вход пришел все-таки бул,
        # надо рассматривать случай бул раньше случая целового числа
        if isinstance(item_to_code, bool):
            return bool_to_bytes()

        if isinstance(item_to_code, int):
            return int_to_bytes()
        
        if isinstance(item_to_code, float):
            return float_to_bytes()
        
        if isinstance(item_to_code, str):
            return string_to_bytes()

        if isinstance(item_to_code, list):
            return list_to_bytes()
        
        if isinstance(item_to_code, dict):
            return dict_to_bytes()
        
        if isinstance(item_to_code, NullableItem):
            if item_to_code.inner_type is bool:
                return bool_to_bytes(is_nullable=True)
            if item_to_code.inner_type is int:
                return int_to_bytes(is_nullable=True)
            if item_to_code.inner_type is float:
                return float_to_bytes(is_nullable=True)
            if item_to_code.inner_type is list:
                return list_to_bytes(is_nullable=True)
            if item_to_code.inner_type is dict:
                return dict_to_bytes(is_nullable=True)
            if item_to_code.inner_type is str:
                return string_to_bytes(is_nullable=True)

        raise ValueError(f"Don't know how to serialize {item_to_code}")
    
    def _recursive_dictionary_decode(self, item_to_decode, n_tabs: int = 0):
        item_type = item_to_decode[0]   # первый байт - всегда тип

        item_type_decoded = self.inverse_codes[item_type]

        if item_type_decoded.startswith("nullable"):
            if item_to_decode[1] == 1:
                # объект = None
                return None
            else:
                item_type_decoded = item_type_decoded[len("nullable_"):]
                item_to_decode = bytearray([item_to_decode[0]]) + item_to_decode[2:]

        if item_type_decoded == "string":
            # следующий байт - является ли строка сокращенной версией
            # некоего ключевого слова из схемы
            shorthand_type = item_to_decode[1]
            if shorthand_type == self.missing_value_int:
                string_len = int.from_bytes(item_to_decode[2:6])
                string_segment = item_to_decode[6:6+string_len].decode('utf-8')
                #print("\t"*n_tabs + f"{item_type_decoded} {string_segment}")
                return string_segment
            else:
                #print("\t"*n_tabs + f"{item_type_decoded} {self.inverse_codes[shorthand_type]}")
                return self.inverse_codes[shorthand_type]
        
        if item_type_decoded == "bool":
            #print("\t"*n_tabs + f"{item_type_decoded} {item_to_decode[1] == 1}")
            return item_to_decode[1] == 1

        if item_type_decoded == "int":
            #print("\t"*n_tabs + f"{item_type_decoded} {int.from_bytes(item_to_decode[1:5])}")
            return int.from_bytes(item_to_decode[1:5])
        
        if item_type_decoded == "float":
            #print("\t"*n_tabs + f"{item_type_decoded} {struct.unpack('d', item_to_decode[1:9])}")
            return struct.unpack("d", item_to_decode[1:9])[0]
        
        if item_type_decoded == "list":
            len_in_bytes = int.from_bytes(item_to_decode[1:5])
            under_segment = item_to_decode[5:5+len_in_bytes]
            i = 4
            seg_len = int.from_bytes(under_segment[0:4])
            items = []
            while i < len_in_bytes:
                items.append(
                    self._recursive_dictionary_decode(under_segment[i:i+seg_len], n_tabs=n_tabs+1)
                )
                i += seg_len
                seg_len = int.from_bytes(under_segment[i:i+4])
                i += 4

            return items

        if item_type_decoded == 'dict':
            len_in_bytes = int.from_bytes(item_to_decode[1:5])
            under_segment = item_to_decode[5:5+len_in_bytes]
            keys = []
            values = []
            seg_len = int.from_bytes(under_segment[0:4])
            
            i = 4
            while i < len_in_bytes:
                #print("\t"*n_tabs + str(i))
                #print(f"Segment length: {seg_len}")
                #print("\t"*n_tabs + f"Decoding key {under_segment[i:i+seg_len]}")
                keys.append(
                    self._recursive_dictionary_decode(under_segment[i:i+seg_len], n_tabs=n_tabs+1)
                )
                i += seg_len
                seg_len = int.from_bytes(under_segment[i:i+4])
                i += 4

                #print("\t"*n_tabs + f"Decoding value {under_segment[i:i+seg_len]}")
                values.append(
                    self._recursive_dictionary_decode(under_segment[i:i+seg_len], n_tabs=n_tabs+1)
                )
                i += seg_len
                seg_len = int.from_bytes(under_segment[i:i+4])
                i += 4
            
            return dict(zip(keys, values))



    def serialize(self, object, out_path: Path):
        results = self._sub_serialize(
            object,
            schema_to_use=self.schema,
            n_tabs=0
        )

        out_content = {k: v for k, v in self.schema.items()}
        out_content['object_data'] = results

        ser_res = self._recursive_dictionary_encode(out_content)
        with open(out_path, "wb") as f:
            f.write(ser_res)


    def _sub_deserialize(self, object, schema_to_use: Union[Dict, None], n_tabs: int = 0):
        if schema_to_use is None:
            schema_to_use = self.schema
            
        if not isinstance(schema_to_use, dict):
            return object

        field_type = schema_to_use['type']
        if field_type.startswith("nullable_"):
            if object is None:
                return None
            else:
                field_type = field_type[len("nullable_"):]

        if field_type == "list":
            val_type = schema_to_use['value_type']
            return [
                self._sub_deserialize(
                    sub_obj,
                    val_type,
                    n_tabs=n_tabs+1
                )
                for sub_obj in object
            ]
        
        if field_type == "dict":
            key_type = schema_to_use['value_type']['keys']
            values_type = schema_to_use['value_type']['values']
            res = {}
            for key, value in object.items():
                k = self._sub_deserialize(
                    key,
                    key_type,
                    n_tabs=n_tabs+1
                )
                v = self._sub_deserialize(
                    value,
                    values_type,
                    n_tabs=n_tabs+2
                )
                res[k] = v
            
            return res

        classname = getattr(sys.modules[self.module], schema_to_use['classname']) # globals()[schema_to_use['classname']]
        class_instance = classname()

        for field in schema_to_use['value_type']:
            setattr(
                class_instance,
                field,
                self._sub_deserialize(
                    object[field],
                    schema_to_use['value_type'][field],
                    n_tabs=n_tabs+1
                )
            )

        return class_instance

    def deserialize(self, in_path: Path):
        with open(in_path, "rb") as r:
            content = r.read()

        parsed_content = self._recursive_dictionary_decode(content)
        
        obj_data = parsed_content['object_data']
        schema = {k: v for k, v in parsed_content.items() if k != "object_data"}

        res = self._sub_deserialize(obj_data, schema)
        return res
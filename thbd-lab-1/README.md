## thbd-lab-1

#### Реализация сериализатора
В рамках работы был реализован бинарный сериализатор, проведено сравнение скорости работы, объемов места на диске между собственной реализацией, AVRO и protobuf.

#### Описание файлов
- Bench results.xlsx -> файл с результатами бенчмарков
- binary_serializer.py -> файл с кодом сериализатора
- classes.py -> файл с кодами классов для сериализации
- run_test.py -> основной исполняемый файл для проведения тестов
- schema_pb2.py -> protobuf схема
- OUT -> директория с выходными файлами
- MySerializerData -> служебная папка для сериализатора
- ProtobufData -> служебная папка для тестов protobuf

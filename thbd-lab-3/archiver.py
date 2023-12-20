from bitarray import bitarray
from pathlib import Path
from typing import Union


def get_best_match(data, current_position, lookahead_len, win_size):

    end_of_buffer = min(current_position + lookahead_len, len(data) + 1)

    best_dist = -1
    best_len = -1

    for j in range(current_position + 2, end_of_buffer):
        # создаем подстрочку для анализа
        start_index = max(0, current_position - win_size)
        substring = data[current_position:j]

        for i in range(start_index, current_position):
            # Ищем сколько раз ключ вошел в текущую строчку
            n_repeats = len(substring) // (current_position - i)

            # Ищем первый символ, не совпавший с ключом
            first_mismatch = len(substring) % (current_position - i)

            # Воссоздаем массив байт
            matched_string = data[i:current_position] * n_repeats + data[i:i+first_mismatch]

            # Если получилось охватить больший объем байт, 
            # переписываем ключ, идем дальше
            if matched_string == substring and len(substring) > best_len:
                best_dist = current_position - i 
                best_len = len(substring)

    if best_dist > 0 and best_len > 0:
        return (best_dist, best_len)
    
    return None


def compress(
        window_size: int,
        lookahead_buffer_len: int,
        input_file: Union[Path, str],
        output_file: Union[Path, str]
):
    data = None
    i = 0
    output_buffer = bitarray(endian='big')

    with open(input_file, 'rb') as instream:
        data = instream.read()

    print(f"Will compress {len(data)} bytes")
    while i < len(data):
        match = get_best_match(data, i, lookahead_buffer_len, window_size)
        if match: 
            (dist, match_len) = match

            # В случае нахождения совпадения в словаре необходимо
            # 1) Добавить соответствующий флаг (1)
            output_buffer.append(True)
            # 2) Указать позицию ключа
            output_buffer.frombytes(bytes([dist >> 4]))
            # 3) Добавить длину совпадения
            output_buffer.frombytes(bytes([((dist & 0xf) << 4) | match_len]))
            
            # Переходим с шагом = длине совпадения
            i += match_len

        else:
            # Если ключа нет, то сама текущая вставка становится ключом
            # Чтобы указать на это ставим флаг нахождения ключа = False (0)
            output_buffer.append(False)
            # И добавляем новый ключ
            output_buffer.frombytes(bytes([data[i]]))
            # Просто идем на следующий бит
            i += 1
        
        if i == len(data) // 4:
            print("25% done")
        elif i == len(data) // 2:
            print("50% done")
        elif i == (len(data) // 4) * 3:
            print("75% done")

    output_buffer.fill()

    with open(output_file, 'wb') as outstream:
        outstream.write(output_buffer.tobytes())


def decompress(
        input_file: Union[Path, str],
        output_file: Union[Path, str]
    ):

        data = bitarray(endian='big')
        buf = []

        with open(input_file, 'rb') as input_file:
            data.fromfile(input_file)

        print(f"Will decode {len(data) // 8} bytes")
        while len(data) >= 9:
            # Загрузка - обратный процесс от кодирования
            # Сначала грузим флаг, смотрим является ли вставка ключом
            flag = data.pop(0)

            if not flag:
                # Если нет, то вписываем следующие 8 бит как есть
                byte = data[0:8].tobytes()

                buf.append(byte)
                del data[0:8]
            else:
                # Если флаг установлен, то вычислим дистанцию до ключа
                # и длину ключа
                byte1 = ord(data[0:8].tobytes())
                byte2 = ord(data[8:16].tobytes())

                del data[0:16]
                dist = (byte1 << 4) | (byte2 >> 4)
                length = (byte2 & 0xf)

                # Вписываем байты в массив
                for i in range(length):
                    buf.append(buf[-dist])

        with open(output_file, 'wb') as output_file:
            output_file.write(b''.join(buf))
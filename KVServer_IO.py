from collections import defaultdict
import os


class IO_Operations:
    locks = defaultdict(int)

    @staticmethod
    def read_file(file_path):
        try:
            while IO_Operations.locks[file_path] == 1:
                continue
            IO_Operations.locks[file_path] = 1
            f = open(file_path, 'r')
            data = f.read()
            f.close()
            IO_Operations.locks[file_path] = 0
            return data
        except:
            raise Exception

    @staticmethod
    def update_file(path, file_name, value):
        file_path = path + file_name
        while IO_Operations.locks[file_path] == 1:
            continue
        IO_Operations.locks[file_path] = 1
        try:
            if os.path.exists(file_path):
                file = open(file_path, 'a')
                file.write(', ' + str(value))
                file.close()
            else:
                file = open(file_path, 'w')
                file.write(str(value))
                file.close()
        except Exception:
            IO_Operations.locks[file_path] = 0
            return 'NOT-STORED'
        IO_Operations.locks[file_path] = 0
        return 'STORED'

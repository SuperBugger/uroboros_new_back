import json
import os
import re
import tarfile
from abc import ABC

from .file_uploader import FileUploader


class DataUploader(FileUploader, ABC):
    def __init__(self):
        super().__init__()
        self.dict_data = {}
        self.data = None
        self._root = None
        self._count = 0

    def get_dict(self, json_data):
        self.data = json.loads(json_data)

    def get_local_data(self, path):
        try:
            _file = open(path)
            self.data = _file.readlines()
        except Exception as ex:
            print(ex)
            self._error("getting data from local ")

    @staticmethod
    def make_temp_directory(name):
        cmd = f'mkdir -p {name}'
        os.system(cmd)

    def decompress_archive(self, path):
        try:
            data = os.path.abspath('data')
            if os.path.isdir(path):
                return None
            self._count += 1
            if path[-3:] == ".7z":
                arch_name = re.split(r"/", path)[-1]
                name = data + "/" + arch_name[:arch_name.index(".")] + f"_{self._count}"
                command = f"7z x {path} - o. {name}"
                if re.search(r"debian", path):
                    name = data + "/" + re.split(r"/", path)[-1] + "-debian"
                    self.make_temp_directory(name)
                    command = f"7z x {path} - o. {name}"
                os.system(command)
                return re.split("/", name)[-1]
            if path[-3:] == "zip":
                arch_name = re.split(r"/", path)[-1]
                name = data + "/" + arch_name[:arch_name.index(".")] + f"_{self._count}"
                command = f'unzip {path} -d {name}'
                if re.search(r"debian", path):
                    name = data + "/" + re.split(r"/", path)[-1] + "-debian"
                    self.make_temp_directory(name)
                    command = f'unzip {path} -d {name}'
                os.system(command)
                return re.split("/", name)[-1]
            if path[-7:] == ".tar.gz" or path[-4:] == ".tgz":
                # print(path)
                arch_name = re.split(r"/", path)[-1]
                name = data + "/" + arch_name[:arch_name.index(".")] + f"_{self._count}"
                command = f"tar -xf {path} -C {name}"
                if re.search(r"debian", path):
                    name = data + "/" + re.split(r"/", path)[-1] + "-debian"
                    self.make_temp_directory(name)
                    command = f"tar -xf {path} -C {name}"
                os.system(command)
                return re.split("/", name)[-1]
            if path[-3:] == ".gz" or path[-4:] == "gzip":
                arch_name = re.split(r"/", path)[-1]
                name = data + "/" + arch_name[:arch_name.index(".")] + f"_{self._count}"
                command = f"gunzip -c {path} > {name}"
                if re.search(r"debian", path):
                    name = "data/" + re.split(r"/", path)[-1] + "-debian"
                    self.make_temp_directory(name)
                    command = f"gunzip -c {path} > {name}"
                os.system(command)
                return re.split("/", name)[-1]
            if tarfile.is_tarfile(path):
                arch_name = re.split(r"/", path)[-1]
                name = data + "/" + arch_name[:arch_name.index(".")] + f"_{self._count}"
                command = f"tar -xf {path} -C {name}"
                if re.search(r"debian", path):
                    name = data + "/" + re.split(r"/", path)[-1] + "-debian"
                    self.make_temp_directory(name)
                    command = f"tar -xf {path} -C {name}"
                os.system(command)
                return re.split("/", name)[-1]
            if path[-4:] == ".bz2":
                arch_name = re.split(r"/", path)[-1]
                name = path[:-4] + f"_{self._count}" + ".bz2"
                os.rename(path, name)
                command = f"bzip2 -d {name}"
                if re.search(r"debian", path):
                    name = data + "/" + re.split(r"/", path)[-1] + "-debian"
                    self.make_temp_directory(name)
                    command = f"bzip2 -d -xf {name}"
                os.system(command)
                return re.split("/", name[:-4])[-1]
            if path[-3:] == ".xz":
                arch_name = re.split(r"/", path)[-1]
                name = data + "/" + arch_name[:arch_name.index(".")] + f"_{self._count}"
                command = f" xz --decompress {path}"
                if re.search(r"debian", path):
                    name = data + "/" + re.split(r"/", path)[-1] + "-debian"
                    self.make_temp_directory(name)
                    command = f"tar -xf {path} -C {name}"
                os.system(command)
                os.rename(path[:-3], name)
                return re.split("/", name)[-1]
            if path[-4:] == ".deb":
                arch_name = re.split(r"/", path)[-1]
                name = data + "/" + arch_name[:arch_name.index(".")] + f"_{self._count}"
                self.make_temp_directory(name)
                command = f"dpkg -x {path} {name}"
                os.system(command)
                print(name)
                return re.split("/", name)[-1]
            if os.path.isfile(path):
                return re.split("/", path)[-1]
            else:
                return None
        except Exception as e:
            print(e)
            self._error("decompress data")

    def clear_trash(self):
        try:
            cmd = 'rm -fr data'
            os.system(cmd)
        except Exception as e:
            print(e)
            self._error("deleting temporary directory")

"""
python3 -m uroboros.cli.manage pkg -r --path <url> --pkg --assembly <assm_id> --noclean
python3 -m uroboros.cli.manage pkg -l --path <path> --pkg --assembly <assm_id>
python3 -m uroboros.cli.manage pkg -l --path <path> --deb --assembly <assm_id>
python3 -m uroboros.cli.manage pkg -l --path <path> --assembly <assm_id>
"""

import os
import re
import shutil
from glob import glob

from .base_uploader import time_decorator
from .changelog_parser import ChangelogUploaderApi


class PkgUploaderApi(ChangelogUploaderApi):
    def __init__(self, db_helper):
        super().__init__(db_helper)
        self._db_helper = db_helper
        self.package = self._ensure_uptable("package", ["pkg_id", "pkg_name"], "pkg_id", [])
        self.pkg_version = self._ensure_uptable("pkg_version", ["pkg_vrs_id", "author_name",
                                                                "pkg_id", "version"
                                                                ], "pkg_vrs_id", ['pkg_id'])
        self.pkg_dict = {"pkg_id": None, "pkg_name": None}
        self.pkg_vrs_dict = {"pkg_vrs_id": None, "pkg_date_created": "", "author_name": "",
                             "pkg_id": None, "version": None}
        self.changelog = self._ensure_uptable("changelog", ["id", "log_desc", "urg_id",
                                                            "pkg_vrs_id", "date_added",
                                                            "log_ident", "rep_name"
                                                            ], "id", ['urg_id', 'pkg_vrs_id'])
        self.assm_pkg_vrs = self._ensure_uptable("assm_pkg_vrs", ["pkg_vrs_id", "assm_id"], ['pkg_vrs_id', 'assm_id'],
                                                 ['pkg_vrs_id', 'assm_id'])
        self.assm_pkg_vrs._schm = "repositories"
        self.urgency = self._ensure_uptable("urgency", ["urg_id", "urg_name"], "urg_id", [])
        self.urgency._schm = "repositories"
        self.changelog._schm = "repositories"
        self.package._schm = "repositories"
        self.pkg_version._schm = "repositories"
        self.pkg_vrs_id_list = []
        self.pkg_desc = {}
        self.pkg_vrs_desc = {}

    @time_decorator
    def check_assembly(self, assm_id):
        sql = f"SELECT EXISTS(SELECT assm_id from repositories.assembly where assm_id={assm_id})"
        print(self._db_helper.query(sql))
        if self._db_helper.query(sql) is None:
            print(f"The assembly with id={assm_id} doesn't exist")
            exit(1)
        if not self._db_helper.query(sql)[0][0]:
            print(f"The assembly with id={assm_id} doesn't exist")
            exit(1)

    @time_decorator
    def processing_assembly_packages(self, assm_id):
        try:
            if len(self.pkg_vrs_id_list) != 0:
                for pkg_vrs_id in self.pkg_vrs_id_list:
                    self.assm_pkg_vrs.upsert((pkg_vrs_id, assm_id), [pkg_vrs_id, assm_id])
                return self.assm_pkg_vrs
        except Exception as e:
            print(e)
            self._error("inserting assm_pkg tables")

    @time_decorator
    def search_remote_packages(self, path, assm_id, only_deb=False, only_pkg=False):
        try:
            count_req = 0
            if path[-1] == "/":
                path = path[:-1]
            for st in ["main", "contrib", "non-free"]:
                if not only_pkg:
                    chng_path = path[:path[:path.rfind("/")].rfind("/")] + "/pool" + "/" + st
                    self.processing_web_object(chng_path, changelog=True)
                if not only_deb:
                    url_pk = path + "/" + st + "/binary-amd64"
                    count_req = self.processing_web_object(url_pk, count_req)
            if count_req == 3:
                print("Wrong url!!!")
                exit(1)
            for file in os.listdir(os.path.abspath('data')):
                name = self.decompress_archive(os.path.abspath('data') + "/" + file)
                if not re.search("changelog", name):
                    self.get_local_data(os.path.abspath('data') + "/" + name)
                    self.processing_remote_packages()
            if not only_pkg:
                chng_tbl = self.source_changelog_walk(up_changelog=[self.package, self.pkg_version])
                for d in chng_tbl:
                    for r in d.rows:
                        self.__dict__[d.name].upsert(r, d.rows[r])
                    self.__dict__[d.name].seq = d.seq
                    self.__dict__[d.name].key2id = d.key2id

            self.processing_assembly_packages(assm_id)
            return self.package, self.assm_pkg_vrs, self.pkg_version, self.urgency, self.changelog
        except Exception as e:
            print(e)
            self._error("working with remote repository")

    @time_decorator
    def processing_remote_packages(self):
        try:
            for line in self.data:
                line = line[:-1]
                if re.search(r'Package:', line) and not re.search(r"Ghc-Package:", line):
                    elem = re.split(" ", line)
                    if "Package:" in elem:
                        if self.pkg_dict["pkg_id"] is not None:
                            self.package.upsert(self.pkg_dict["pkg_id"], [self.pkg_dict["pkg_id"],
                                                                          self.pkg_dict["pkg_name"]])
                        if self.pkg_vrs_dict["pkg_vrs_id"] is not None:
                            self.pkg_version.upsert(self.pkg_vrs_dict["pkg_vrs_id"], [self.pkg_vrs_dict["pkg_vrs_id"],
                                                                                      self.pkg_vrs_dict["author_name"],
                                                                                      self.pkg_vrs_dict["pkg_id"],
                                                                                      self.pkg_vrs_dict["version"]])
                        self.pkg_dict = {"pkg_id": None, "pkg_name": None, "ref_auth": None}
                        self.pkg_vrs_dict = {"pkg_vrs_id": None, "pkg_date_created": None, "author_name": None,
                                             "pkg_id": None, "version": None}
                        self.pkg_dict["pkg_name"] = line[9:]
                        self.pkg_dict["pkg_id"] = self.package.getid(self.pkg_dict["pkg_name"],
                                                                     sql_attr="pkg_name")
                    continue

                if re.search(r'Version:', line) and self.pkg_dict["pkg_id"] is not None:
                    elem = re.split(" ", line)
                    if "Version:" in elem:
                        self.pkg_vrs_dict["pkg_id"] = self.pkg_dict["pkg_id"]
                        self.pkg_vrs_dict["version"] = line[9:]
                        self.pkg_vrs_dict["pkg_vrs_id"] = self.pkg_version.getid((self.pkg_vrs_dict["version"],
                                                                                  self.pkg_dict["pkg_id"]),
                                                                                 sql_attr=[
                                                                                     "version",
                                                                                     "pkg_id"])
                        self.pkg_vrs_id_list.append(self.pkg_vrs_dict["pkg_vrs_id"])
                    continue

                if re.search(r"Original-Maintainer", line) and self.pkg_dict["pkg_id"] is not None:
                    self.pkg_vrs_dict["author_name"] = line[21:]
                    self.pkg_vrs_desc[self.pkg_vrs_dict["pkg_vrs_id"]] = line[21:]

        except Exception as e:
            self._error(f"processing remote repository. Packages.{e}")

    # downloading package information from source repository
    @time_decorator
    def processing_dsc_files(self):
        try:
            self.pkg_dict = {"pkg_id": None, "pkg_name": None}
            self.pkg_vrs_dict = {"pkg_vrs_id": None, "author_name": None,
                                 "pkg_id": None, "version": None}
            for line in self.data:
                if line.startswith("Source:"):
                    self.pkg_dict["pkg_name"] = line[8:-1]
                    self.pkg_dict["pkg_id"] = self.package.getid(line[8:-1], sql_attr="pkg_name")
                    continue
                if line.startswith("Version:"):
                    self.pkg_vrs_dict["version"] = line[9:-1]
                    pkg_vrs_id = self.pkg_version.getid((self.pkg_vrs_dict["version"], self.pkg_dict["pkg_id"]),
                                                        sql_attr=["version", "pkg_id"])
                    self.pkg_vrs_dict["pkg_id"] = self.pkg_dict["pkg_id"]
                    self.pkg_vrs_dict["pkg_vrs_id"] = pkg_vrs_id
                    self.pkg_vrs_id_list.append(pkg_vrs_id)
                    continue
                if self.data.index(line) == len(self.data) - 1:
                    self.package.upsert(self.pkg_dict["pkg_id"], [self.pkg_dict["pkg_id"],
                                                                  self.pkg_dict["pkg_name"]])
                    self.pkg_version.upsert(self.pkg_vrs_dict["pkg_vrs_id"], [self.pkg_vrs_dict["pkg_vrs_id"],
                                                                              self.pkg_vrs_dict["author_name"],
                                                                              self.pkg_vrs_dict["pkg_id"],
                                                                              self.pkg_vrs_dict["version"]])
        except Exception as e:
            self._error(f"processing local repository. .dsc.{e}")

    @time_decorator
    def searching_local_packages(self, path, assm_id, only_pkg=False):
        try:
            for root, dirs, files in os.walk(path):
                for dir in dirs:
                    dir_path = path + "/" + dir
                    if dir in os.listdir(path):
                        for file in os.listdir(dir_path):
                            if file[-3:] == "dsc":
                                self.get_local_data(dir_path + "/" + file)
                                self.processing_dsc_files()
                            else:
                                if re.search("debian", file):
                                    self.decompress_archive(dir_path + "/" + file)
                    else:
                        if not only_pkg:
                            self.processing_deb_packages(path, assm_id, is_all=True)
                        chnglog = ChangelogUploaderApi(self._db_helper)
                        dists = chnglog.source_changelog_walk(up_changelog=[self.package, self.pkg_version])
                        for d in dists:
                            for r in d.rows:
                                self.__dict__[d.name].upsert(r, d.rows[r])
                            self.__dict__[d.name].seq = d.seq
                            self.__dict__[d.name].key2id = d.key2id
                        self.processing_assembly_packages(assm_id)
                        return [self.package, self.assm_pkg_vrs, self.pkg_version, self.urgency, self.changelog]

        except Exception as e:
            self._error(f"processing local repository. Search packages.{e}")

    # downloading .deb package information
    @time_decorator
    def processing_deb_packages(self, path, assm_id, is_all=False):
        try:
            data = os.path.abspath('data')
            for file_name in glob(path + '/**/*.deb', recursive=True):
                shutil.copy(file_name, data)
                file_name = re.split("/", file_name)[-1]
                pkg_desc = re.split("_", file_name)
                pkg_id = self.package.getid(pkg_desc[0], sql_attr="pkg_name")
                pkg_vrs_id = self.pkg_version.getid((pkg_desc[1],
                                                     pkg_id), sql_attr=["version", "pkg_id"])
                self.pkg_vrs_id_list.append(pkg_vrs_id)
                self.package.upsert(pkg_id, [pkg_id, pkg_desc[0]])
                self.pkg_version.upsert(pkg_vrs_id, [pkg_vrs_id, None, pkg_id, pkg_desc[1]])
                if not is_all:
                    self.decompress_archive(data + "/" + file_name)
            if not is_all:
                dists = self.source_changelog_walk(up_changelog=[self.package, self.pkg_version])
                for d in dists:
                    for r in d.rows:
                        self.__dict__[d.name].upsert(r, d.rows[r])
                    self.__dict__[d.name].seq = d.seq
                    self.__dict__[d.name].key2id = d.key2id
                self.processing_assembly_packages(assm_id)
            return [self.package, self.assm_pkg_vrs, self.pkg_version, self.urgency, self.changelog]
        except Exception as e:
            self._error(f"processing repository .deb.{e}")

    @time_decorator
    def run(self, args):
        self.make_temp_directory("data")
        if args.assembly is not None:
            if args.l:
                if args.path is not None:
                    if re.search(r'\w', args.path):
                        if args.path[-1] == "/":
                            args.path = args.path[:-1]
                        if args.path[0] == ".":
                            args.path = os.path.realpath(args.path)
                    if not os.path.isdir(args.path):
                        print("Wrong path!")
                        exit(1)
                    self.check_assembly(args.assembly)
                    if args.deb:
                        self.processing_deb_packages(args.path, args.assembly)
                    if args.pkg:
                        self.searching_local_packages(args.path, args.assembly, only_pkg=True)
                    else:
                        self.searching_local_packages(args.path, args.assembly)
                else:
                    self._error("Wrong path")
            if args.r:
                if args.path is not None:
                    self.check_assembly(args.assembly)
                    self.search_remote_packages(args.path, args.assembly, only_deb=args.deb, only_pkg=args.pkg)
                else:
                    self._error("Wrong path")
            if not args.l and not args.r:
                self._error("!Wrong args!")
        else:
            self._error("Packages without assembly")
        self._upload_tables()
        if not args.noclean:
            self.clear_trash()
        print("Successful")

"""
python3 -m uroboros.cli.manage assm -l --path <path>
python3 -m uroboros.cli.manage assm -l --path <path> --pkg
python3 -m uroboros.cli.manage assm -l --path <path> --deb
python3 -m uroboros.cli.manage assm -r --path <url>
python3 -m uroboros.cli.manage assm -r --path https://ppa.launchpadcontent.net/ubuntu-wine/ppa/ubuntu/dists/jaunty --pkg
                                        --date_created <date>
python3 -m uroboros.cli.manage assm -r --path https://ppa.launchpadcontent.net/ubuntu-wine/ppa/ubuntu/dists/jaunty
                                        --pkg --date_created <date> --noclean
python3 -m uroboros.cli.manage assm --delete --assembly <assm_id>
"""

import os
import re
from datetime import datetime

from .base_uploader import time_decorator
from .data_uploader import DataUploader
from .pkg_uploader import PkgUploaderApi
from .project_uploader import ProjectUploaderApi


class AssemblyUploaderApi(DataUploader):
    def __init__(self, db_helper):
        super().__init__()
        self._db_helper = db_helper
        self.pkg = PkgUploaderApi(self._db_helper)
        self.up_assm = self._ensure_uptable("assembly", ["assm_id", "assm_date_created", "assm_desc",
                                                         "prj_id", "assm_version"], "assm_id", ['prj_id'])
        self.up_assm_dict = {"assm_id": None, "assm_date_created": str(datetime.now()), "assm_desc": None,
                             "prj_id": None, "assm_version": None}
        self.project = self._ensure_uptable("project", ["prj_id", "prj_name", "rel_id",
                                                        "prj_desc", "vendor", "arch_id"], "prj_id", ["rel_id"])
        self.changelog = self._ensure_uptable("changelog", ["id", "log_desc", "urg_id",
                                                            "pkg_vrs_id", "date_added",
                                                            "log_ident", "rep_name"
                                                            ], "id", ['urg_id', 'pkg_vrs_id'])
        self.assm_pkg_vrs = self._ensure_uptable("assm_pkg_vrs", ["pkg_vrs_id", "assm_id"], ['pkg_vrs_id', 'assm_id'],
                                                 ['pkg_vrs_id', 'assm_id'])
        self.package = self._ensure_uptable("package", ["pkg_id", "pkg_name"], "pkg_id", [])
        self.pkg_version = self._ensure_uptable("pkg_version", ["pkg_vrs_id", "author_name",
                                                                "pkg_id", "version"
                                                                ], "pkg_vrs_id", ['pkg_id'])
        self.urgency = self._ensure_uptable("urgency", ["urg_id", "urg_name"], "urg_id", [])
        self.project._schm = "repositories"
        self.up_assm._schm = "repositories"
        self.changelog._schm = "repositories"
        self.assm_pkg_vrs._schm = "repositories"
        self.package._schm = "repositories"
        self.pkg_version._schm = "repositories"
        self.urgency._schm = "repositories"

    @time_decorator
    def delete_assembly(self, assm_id):
        try:
            result_sql = ""
            pkg_del = []
            pkg_vrs_del = []
            result_sql += f"DELETE FROM repositories.assembly WHERE assm_id = {assm_id};"
            self._db_helper.query(result_sql)
            self._db_helper.commit_conn()
            sql = f"select pkg_vrs_id, pkg_id from repositories.pkg_version " \
                  f"where pkg_vrs_id not in (SELECT pkg_vrs_id " \
                  f"from repositories.assm_pkg_vrs where assm_id={assm_id})"
            for pkg_list in self._db_helper.query(sql):
                sql = f"select count(*) from repositories.assm_pkg_vrs rapv" \
                      f" join repositories.pkg_version rpv on rpv.pkg_vrs_id = rapv.pkg_vrs_id" \
                      f" join repositories.package rp on rpv.pkg_id=rp.pkg_id and rp.pkg_id={pkg_list[1]} "
                if self._db_helper.query(sql)[0][0] < 1:
                    pkg_del.append(pkg_list[1])
                    pkg_vrs_del.append(pkg_list[0])
            result_sql = ""
            for p_id in pkg_vrs_del:
                result_sql += f"DELETE FROM repositories.pkg_version WHERE pkg_vrs_id = {p_id};"
            for p_id in pkg_del:
                result_sql += f"DELETE FROM repositories.package WHERE pkg_id = {p_id};"
            if len(result_sql) != 0:
                self._db_helper.query(result_sql)
                self._db_helper.commit_conn()
                result_sql = ""
            sql = f"select arch_id from repositories.architecture " \
                  f"where arch_id not in (select arch_id from repositories.project)"
            if len(self._db_helper.query(sql)) != 0:
                arch_list = self._db_helper.query(sql)
                for arch_id in arch_list:
                    result_sql += f"DELETE FROM repositories.architecture WHERE arch_id = {arch_id[0]};"
            sql = f"select rel_id from repositories.release " \
                  f"where rel_id not in (select rel_id from repositories.project)"
            if len(self._db_helper.query(sql)) != 0:
                rel_list = self._db_helper.query(sql)
                if rel_list[0] is not None:
                    for arch_id in rel_list:
                        result_sql += f"DELETE FROM repositories.release WHERE rel_id = {arch_id[0]};"
            sql = f"select urg_id from repositories.urgency " \
                  f"where urg_id not in (select urg_id from repositories.changelog)"
            if len(self._db_helper.query(sql)) != 0:
                urg_list = self._db_helper.query(sql)
                if urg_list[0] is not None:
                    for arch_id in urg_list:
                        result_sql += f"DELETE FROM repositories.urgency WHERE urg_id = {arch_id[0]};"
            if len(result_sql) != 0:
                self._db_helper.query(result_sql)
                self._db_helper.commit_conn()
        except Exception as e:
            print(e)
            self._error("Deleting project")

    @time_decorator
    def check_assembly(self, prj_name=None, prj_id=None, assm_id=None):
        try:
            if assm_id is not None:
                sql = f"SELECT EXISTS(SELECT * " \
                      f"from repositories.assembly " \
                      f"where assm_id={assm_id});"
                if self._db_helper.query(sql) is None:
                    print("This assembly doesn't exist")
                    exit(1)
                if self._db_helper.query(sql)[0][0]:
                    return
                else:
                    print("This assembly doesn't exist")
                    exit(1)
            else:
                sql = f"SELECT EXISTS(SELECT prj_name, assm_version, assm_date_created " \
                      f"from repositories.assembly ra " \
                      f"join repositories.project rp " \
                      f"on ra.prj_id = rp.prj_id " \
                      f"and rp.prj_name='{prj_name}' " \
                      f"and ra.assm_version='{self.up_assm_dict['assm_version']}' " \
                      f"and ra.assm_date_created='{self.up_assm_dict['assm_date_created']}');"
                if prj_id is not None:
                    self.up_assm_dict["prj_id"] = prj_id
                    sql = f"SELECT EXISTS(SELECT prj_id, assm_version " \
                          f"from repositories.assembly ra " \
                          f"WHERE prj_id={self.up_assm_dict['prj_id']} " \
                          f"and assm_version='{self.up_assm_dict['assm_version']}' " \
                          f"and assm_date_created='{self.up_assm_dict['assm_date_created']}');"
                else:
                    self.up_assm_dict["prj_id"] = self.project.getid(prj_name, sql_attr="prj_name")
                if self._db_helper.query(sql) is None:
                    self.up_assm_dict["assm_id"] = self.up_assm.getid((self.up_assm_dict["prj_id"],
                                                                       self.up_assm_dict["assm_version"],
                                                                       self.up_assm_dict['assm_date_created']),
                                                                      sql_attr=["prj_id", "assm_version",
                                                                                "assm_date_created"])
                    return
                if self._db_helper.query(sql)[0][0]:
                    print("The assembly already exists. Add assembly with today's date")
                    self.up_assm_dict["assm_date_created"] = str(datetime.now())
                    self.up_assm_dict["assm_id"] = self.up_assm.getid((self.up_assm_dict["prj_id"],
                                                                       self.up_assm_dict["assm_version"],
                                                                       self.up_assm_dict['assm_date_created']),
                                                                      sql_attr=["prj_id", "assm_version",
                                                                                "assm_date_created"])
                    return
                else:
                    self.up_assm_dict["assm_id"] = self.up_assm.getid((self.up_assm_dict["prj_id"],
                                                                       self.up_assm_dict["assm_version"],
                                                                       self.up_assm_dict['assm_date_created']),
                                                                      sql_attr=["prj_id", "assm_version",
                                                                                "assm_date_created"])
        except Exception as e:
            print(e)
            self._error("Assembly check")

    @time_decorator
    def check_project(self, prj_id):
        sql = f"SELECT EXISTS(SELECT prj_id from repositories.project where prj_id={prj_id})"
        if self._db_helper.query(sql) is None:
            print(f"The project with id={prj_id} doesn't exist")
            exit(1)
        if not self._db_helper.query(sql)[0][0]:
            print(f"The project with id={prj_id} doesn't exist")
            exit(1)

    @time_decorator
    def processing_local_assembly(self, args):
        try:
            if re.search(r'\w', args.path):
                if args.path[-1] == "/":
                    args.path = args.path[:-1]
                if args.path[0] == ".":
                    args.path = os.path.realpath(args.path)
            print(args.path)
            if not os.path.isdir(args.path):
                print("Wrong path!")
                exit(1)
            self.up_assm_dict["assm_desc"] = args.path
            assm = os.path.split(args.path)[1]
            if args.date_created is None:
                self.up_assm_dict['assm_date_created'] = str(datetime.now())
            else:
                self.up_assm_dict['assm_date_created'] = ' '.join(args.date_created)
            if assm.find("-"):
                assm_desc = re.split("-", assm)
                self.up_assm_dict["assm_version"] = assm_desc[1]
            if args.project is None:
                prj_name = assm
                if assm.find("-"):
                    prj_name = re.split("-", assm)[0]
                self.check_assembly(prj_name=prj_name)
                self.project.upsert(self.up_assm_dict["prj_id"], [self.up_assm_dict["prj_id"], prj_name, None, None,
                                                                  None, None])
            else:
                self.check_project(args.project)
                self.check_assembly(prj_id=args.project)
            self.up_assm.upsert(self.up_assm_dict["assm_id"],
                                    [self.up_assm_dict["assm_id"],
                                     self.up_assm_dict['assm_date_created'], self.up_assm_dict["assm_desc"],
                                     self.up_assm_dict["prj_id"], self.up_assm_dict["assm_version"]])
            if args.deb or args.pkg:
                if args.pkg:
                    pkg_tables = self.pkg.searching_local_packages(args.path, self.up_assm_dict["assm_id"],
                                                                   only_pkg=args.pkg)
                else:
                    pkg_tables = self.pkg.processing_deb_packages(args.path, self.up_assm_dict["assm_id"])
            else:
                pkg_tables = self.pkg.searching_local_packages(args.path, self.up_assm_dict["assm_id"])
            for d in pkg_tables:
                for r in d.rows:
                    self.__dict__[d.name].upsert(r, d.rows[r])
        except Exception as e:
            print(e)
            self._error("working with local repository. Processing assembly")

    @time_decorator
    def remote_assm(self, args):
        try:
            for line in self.data:
                re.sub(r'/n', r'', line)
                if re.search(r"Origin", line):
                    self.up_assm_dict["prj_id"] = args.project
                    if args.project is None:
                        proj = ProjectUploaderApi(self._db_helper)
                        prj_tbl = proj.processing_project_name(line[8:])
                        for r in prj_tbl.rows:
                            self.__dict__[prj_tbl.name].upsert(r, prj_tbl.rows[r])
                            self.up_assm_dict["prj_id"] = prj_tbl.rows[r][0]
                    else:
                        self.check_project(self.up_assm_dict["prj_id"])
                    continue
                if re.search(r'Version:', line):
                    self.up_assm_dict["assm_version"] = line[9:]
                    continue
                if re.search(r'Description:', line):
                    self.up_assm_dict["assm_desc"] = line[13:]
                    continue
                if re.search(r'Date:', line):
                    self.up_assm_dict["assm_date_created"] = line[6:]
                    continue
                if None not in self.up_assm_dict.values():
                    break
            if args.date_created is not None:
                self.up_assm_dict["assm_date_created"] = ' '.join(args.date_created)
            self.check_assembly(prj_id=self.up_assm_dict["prj_id"])
            self.up_assm.upsert(self.up_assm_dict["assm_id"],
                                [self.up_assm_dict["assm_id"], self.up_assm_dict["assm_date_created"],
                                 self.up_assm_dict["assm_desc"], self.up_assm_dict["prj_id"],
                                 self.up_assm_dict["assm_version"]])
            dists = self.pkg.search_remote_packages(args.path, self.up_assm_dict["assm_id"], only_deb=args.deb,
                                                    only_pkg=args.pkg)
            for d in dists:
                for r in d.rows:
                    print(d.rows[r])
                    self.__dict__[d.name].upsert(r, d.rows[r])
        except Exception as e:
            print(e)
            self._error("working with remote repository, processing Release")

    @time_decorator
    def run(self, args):
        if args.delete is not None:
            self.check_assembly(assm_id=args.delete)
            self.delete_assembly(assm_id=args.delete)
            print("Successful")
            return True
        self.make_temp_directory("data")
        if args.l:
            if args.path is not None:
                self.processing_local_assembly(args)
            else:
                self._error("Wrong path")
        if args.r:
            if args.path is not None:
                if args.path[-1] == "/":
                    args.path = args.path[:-1]
                self.processing_web_object(args.path + "/Release")
                self.remote_assm(args)
            else:
                self._error("Wrong path")
        if not args.r and not args.l:
            self._error("!Wrong args!")
        self._upload_tables()
        if not args.noclean:
            self.clear_trash()
        print("Successful")
        return True

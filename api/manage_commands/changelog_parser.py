import os
import re
from glob import glob

from .data_uploader import DataUploader

flag = 0
flag_end = 0


class ChangelogUploaderApi(DataUploader):
    def __init__(self, db_helper):
        super().__init__()
        self._db_helper = db_helper
        self.changelog = self._ensure_uptable("changelog", ["id", "log_desc", "urg_id",
                                                            "pkg_vrs_id", "date_added",
                                                            "log_ident",
                                                            "rep_name"
                                                            ], "id", ['pkg_vrs_id', 'urg_id'])
        self.urgency = self._ensure_uptable("urgency", ["urg_id", "urg_name"], "urg_id", [])
        self.package = self._ensure_uptable("package", ["pkg_id", "pkg_name"
                                                        ], "pkg_id", [])
        self.pkg_version = self._ensure_uptable("pkg_version", ["pkg_vrs_id", "author_name",
                                                                "pkg_id", "version"
                                                                ], "pkg_vrs_id", ['pkg_id'])
        self.urgency._schm = "repositories"
        self.changelog._schm = "repositories"
        self.pkg_version._schm = "repositories"
        self.package._schm = "repositories"

    def source_changelog_walk(self, up_changelog=None):
        dikts = []
        data = os.path.abspath('data')
        for d in up_changelog:
            for r in d.rows:
                self.__dict__[d.name].upsert(r, d.rows[r])
            self.__dict__[d.name].seq = d.seq
            self.__dict__[d.name].key2id = d.key2id
        for file_name in glob('data/**/changelog.Debian.gz', recursive=True):
            name = self.decompress_archive(file_name)
            self.get_local_data(data + "/" + name)
            dikts = self.changelog_uploader()
        for file_name in glob('data/**/debian/changelog', recursive=True):
            self.get_local_data(file_name)
            dikts = self.changelog_uploader()
        return dikts

    def changelog_uploader(self):
        try:
            log_dict = {"id": None, "log_desc": "", "urg_id": None,
                        "pkg_vrs_id": None, "log_author": "",
                        "date_added": "", "log_ident": "", "rep_name": None}
            log_urg = {"urg_id": None, "urg_name": None}
            log_pkg_dict = {"pkg_id": None, "pkg_name": None}
            log_pkg_vrs_dict = {"pkg_vrs_id": None, "author_name": "",
                                "pkg_id": None, "version": None}
            for line in self.data:
                if re.search(r'urgency', line):
                    if log_dict["id"] is not None:
                        self.package.upsert(log_pkg_dict["pkg_id"],
                                            [log_pkg_dict["pkg_id"], log_pkg_dict["pkg_name"]])
                        self.pkg_version.upsert(log_pkg_vrs_dict["pkg_vrs_id"],
                                                [log_pkg_vrs_dict["pkg_vrs_id"],
                                                 log_pkg_vrs_dict["author_name"],
                                                 log_pkg_vrs_dict["pkg_id"],
                                                 log_pkg_vrs_dict["version"]])
                        self.urgency.upsert(log_urg["urg_id"], [log_urg["urg_id"], log_urg["urg_name"]])
                        self.changelog.upsert(log_dict["id"],
                                              [log_dict["id"], log_dict["log_desc"], log_dict["urg_id"],
                                               log_dict["pkg_vrs_id"],
                                               log_dict["date_added"],
                                               log_dict["log_ident"], log_dict["rep_name"]])
                    log_dict = {"id": None, "log_desc": "", "urg_id": None,
                                "pkg_vrs_id": None, "log_author": "",
                                "date_added": "", "is_vul": False, "log_ident": "", "rep_id": None}
                    log_pkg_dict = {"pkg_id": None, "pkg_name": None}
                    log_pkg_vrs_dict = {"pkg_vrs_id": None, "pkg_date_created": None, "author_name": "",
                                        "pkg_id": None, "version": None}
                    lines = line
                    line = re.split(' ', line)
                    lines = re.split(";", lines)[-1]
                    log_pkg_dict["pkg_name"] = line[0]  # name_pkg
                    log_pkg_dict["pkg_id"] = self.package.getid(log_pkg_dict["pkg_name"], sql_attr="pkg_name")
                    log_pkg_vrs_dict["version"] = line[1][1:-1]  # version
                    log_dict["pkg_vrs_id"] = self.pkg_version.getid((log_pkg_vrs_dict["version"],
                                                                     log_pkg_dict["pkg_id"]),
                                                                    sql_attr=["version", "pkg_id"])
                    log_pkg_vrs_dict["pkg_vrs_id"] = log_dict["pkg_vrs_id"]
                    log_pkg_vrs_dict["pkg_id"] = log_pkg_dict["pkg_id"]
                    log_dict["rep_name"] = line[2][:-1]  # release
                    log_urg["urg_id"] = self.urgency.getid(lines[9:-1], sql_attr="urg_name")
                    log_urg["urg_name"] = lines[9:-1]
                    log_dict["urg_id"] = log_urg["urg_id"]
                    log_dict["id"] = self.changelog.getid((log_dict["pkg_vrs_id"],
                                                           log_dict["rep_name"]),
                                                          sql_attr=["pkg_vrs_id", "rep_name"])

                    continue
                if (line.find('*') != -1 or line.find('-') != -1 or line.find('+') != -1) \
                        and log_dict["id"] is not None and not re.search("--", line):
                    log_dict["log_desc"] += line
                    desc_lines = re.split(r" ", line)
                    for desc in desc_lines:
                        if desc.find('CVE-') != -1:
                            while desc[-1].isalpha() or re.search(r'[.,!?:;)(\t\n_\-\'\"]', desc[-1]) or desc.count(
                                    "-") != 2:
                                desc = desc[:-1]
                            log_dict["log_ident"] += " " + desc[desc.index("C"):] + ","
                    continue

                if not line.startswith(" --") and log_dict["id"] is not None:
                    log_dict["log_desc"] += line[1:]
                    desc_lines = re.split(r" ", line)
                    for desc in desc_lines:
                        if desc.find('CVE-') != -1:
                            while desc[-1].isalpha() or re.search(r'[.,!?:;)(\t\n_\-\'\"]', desc[-1]) or desc.count(
                                    "-") != 2:
                                desc = desc[:-1]
                            log_dict["log_ident"] += " " + desc[desc.index("C"):]
                            log_dict["log_ident"] += ","
                    # log_dict["log_ident"]
                    continue
                if line.startswith("  ["):
                    elems = re.split(r" ", line)
                    log_pkg_vrs_dict["author_name"] += elems[2] + elems[3]
                    continue
                if re.search("--", line) and log_dict["id"] is not None:
                    log_pkg_vrs_dict["author_name"] += line[4:line.index(">") + 1]
                    log_dict["date_added"] += line[line.index(">") + 2:]
            return self.pkg_version, self.package, self.changelog, self.urgency
        except Exception as e:
            raise e

    def run(self, path=None):
        self.source_changelog_walk()
    # def run(self, path=None):
    #     from .assembly_uploader import AssemblyUploaderApi
    #     assm = AssemblyUploaderApi(self._db_helper)
    #     assm.run(args)

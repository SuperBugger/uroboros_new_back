import re

from ..query_commands.base_query import BaseApi, time_decorator


class CveApi(BaseApi):
    def __init__(self, db_helper):
        super().__init__()
        self._db_helper = db_helper
        self.table_name = "repositories.pkg_version rpv "
        self.name_col = ["cve_id", "cve_name", "cve_desc", "rep_name", "urg_name", "st_name", "pkg_version"]
        self.fields = "dc.cve_id, dc.cve_name, dc.cve_desc, dr.rep_name, du.urg_name, ds.st_name, rpv.pkg_vrs_id"

    @time_decorator
    def delete_table(self, name):
        try:
            sql = f"SELECT EXISTS(SELECT * FROM pg_tables WHERE tablename = '{name}'); "
            if self._db_helper.query(sql)[0][0]:
                sql = f"DROP TABLE maintenance.{name}"
                self._db_helper.query(sql)
                self._db_helper.commit_conn()
                exit()
            else:
                print("Table doesn't exist")
        except Exception as e:
            self._error(f" while drop table {e}")

    @time_decorator
    def get_pkg_cve(self, args):
        try:
            sql = f"select * from maintenance.get_pkg_cve({args.resolved}, {args.pkg_vrs_id})"
            where = "where "
            where_list = []
            if args.urgency is not None:
                str_urg = ""
                str_urg += " urg_name like '%" + args.urgency[0] + "%'"
                if len(args.urgency) > 1:
                    for x in range(1, len(args.urgency) - 1):
                        str_urg += " or urg_name like '%" + args.urgency[x] + "%'"
                where_list.append(str_urg)

            if args.status is not None:
                str_urg = ""
                for x in range(0, len(args.status) - 1):
                    str_urg += "'" + args.status[x] + "'" + ","
                str_urg += "'" + args.status[-1] + "'"
                where_list.append(f"st_name in ({str_urg})")

            if args.severity is not None:
                str_urg = ""
                str_urg += " severity like '" + args.severity[0] + "%'"
                if len(args.severity) > 1:
                    for x in range(1, len(args.severity) - 1):
                        str_urg += " or severity like '" + args.severity[x] + "%'"
                where_list.append(str_urg)

            if args.fdate is not None and args.sdate is not None:
                fdate = "".join(x + " " for x in args.fdate)
                sdate = "".join(x + " " for x in args.sdate)
                where_list.append(f"bv.date_discovered between '{fdate}' and '{sdate}'")
            if args.fdate is not None and args.sdate is None:
                fdate = "".join(x + " " for x in args.fdate)
                where_list.append(f"bv.date_discovered >='{fdate}' ")
            if args.sdate is not None and args.fdate is None:
                sdate = "".join(x + " " for x in args.sdate)
                where_list.append(f"bv.date_discovered <='{sdate}' ")
            if len(where_list) != 0:
                where += where_list[0]
                for param in range(1, len(where_list)):
                    where += " and " + where_list[param]
                sql += f" {where}"

            self.query = sql
            self.name_col = ["pkg_name", "pkg_vers", "deb_vers", "cve_name", "cve_desc",
                             "st_name", "urg_name", "rep_name", "link",
                             "vul_ident", "vul_name", "vul_desc", "date_discovered",
                             "cvss2_vector", "cvss2_score", "cvss3_vector", "cvss3_score", "severity",
                             "cwe_name", "url"]
        except Exception as e:
            self._error(e)

    @time_decorator
    def joint_assm_vul(self, args):
        where = "where "
        where_list = []
        try:
            sql = "SELECT EXISTS(SELECT * FROM pg_tables WHERE tablename = 'assm_vul'); "
            if not self._db_helper.query(sql)[0][0]:
                sql = (" create table maintenance.assm_vul("
                       " pkg_vul_id integer, pkg_name text, joint_vers text, "
                       "assm_date timestamptz, "
                       "cve_vers text, "
                       "cve_name text, cve_desc text,st_name text, "
                       "urg_name text, rep_name text, link text, "
                       "vul_ident text, vul_name text, vul_desc text, "
                       "date_discovered timestamptz, cvss2_vector text, cvss2_score text, "
                       "cvss3_vector text, cvss3_score text, "
                       "severity text, cwe_name text, url text);")
                self._db_helper.query(sql)
                self._db_helper.commit_conn()
            sql = f"call maintenance.get_joint_assm('{args.assm_id}');"
            self._db_helper.query(sql)
            self._db_helper.commit_conn()

            if args.urgency is not None:
                str_urg = ""
                str_urg += " urg_name like '%" + args.urgency[0] + "%'"
                if len(args.urgency) > 1:
                    for x in range(1, len(args.urgency) - 1):
                        str_urg += " or urg_name like '%" + args.urgency[x] + "%'"
                where_list.append(str_urg)

            if args.status is not None:
                str_urg = ""
                for x in range(0, len(args.status) - 1):
                    str_urg += "'" + args.status[x] + "'" + ","
                str_urg += "'" + args.status[-1] + "'"
                where_list.append(f"st_name in ({str_urg})")

            if args.severity is not None:
                str_urg = ""
                str_urg += " severity like '" + args.severity[0] + "%'"
                if len(args.severity) > 1:
                    for x in range(1, len(args.severity) - 1):
                        str_urg += " or severity like '" + args.severity[x] + "%'"
                where_list.append(str_urg)

            if args.fdate is not None and args.sdate is not None:
                fdate = "".join(x + " " for x in args.fdate)
                sdate = "".join(x + " " for x in args.sdate)
                where_list.append(f"bv.date_discovered between '{fdate}' and '{sdate}'")
            if args.fdate is not None and args.sdate is None:
                fdate = "".join(x + " " for x in args.fdate)
                where_list.append(f"bv.date_discovered >='{fdate}' ")
            if args.sdate is not None and args.fdate is None:
                sdate = "".join(x + " " for x in args.sdate)
                where_list.append(f"bv.date_discovered <='{sdate}' ")

            sql = (f"select pkg_name,  joint_vers, cve_name, cve_desc,"
                   f" st_name, urg_name, rep_name, link, vul_ident, vul_name, vul_desc, date_discovered,  "
                   f"cvss2_vector, cvss2_score, cvss3_vector, cvss3_score, severity, cwe_name, "
                   f"url from maintenance.assm_vul")

            if len(where_list) != 0:
                where += where_list[0]
                for param in range(1, len(where_list)):
                    where += " and " + where_list[param]
                sql += f" {where}"
            if isinstance(args.pkg_vul_id, int):
                if len(where_list) != 0:
                    sql += f" and pkg_vul_id = {args.pkg_vul_id}"
                else:
                    sql += f" where pkg_vul_id = {args.pkg_vul_id}"
            self.name_col = ["pkg_name", "joint_vers", "cve_name", "cve_desc",
                             "st_name", "urg_name", "rep_name", "link",
                             "vul_ident", "vul_name", "vul_desc", "date_discovered",
                             "cvss2_vector", "cvss2_score", "cvss3_vector", "cvss3_score", "severity",
                             "cwe_name", "url"]
            self.query = sql
            print(self.query)
        except Exception as e:
            self._error(f" while get cve of joint assm {e}")

    @time_decorator
    def assm_vul(self, args):
        try:
            sql = f"select * from maintenance.get_assm_cve({args.resolved}, {args.assm_id})"
            self.name_col = ["pkg_name", "cve_vers", "joint_vers", "cve_name", "cve_desc",
                             "st_name", "urg_name", "rep_name", "link",
                             "vul_ident", "vul_name", "vul_desc", "date_discovered",
                             "cvss2_vector", "cvss2_score", "cvss3_vector", "cvss3_score", "severity",
                             "cwe_name", "url"]
            where = "where "
            where_list = []
            print(args.urgency)
            if args.urgency is not None:
                print(args.urgency)
                str_urg = ""
                str_urg += " urg_name like '%" + args.urgency[0] + "%'"
                if len(args.urgency) > 1:
                    for x in range(1, len(args.urgency) - 1):
                        str_urg += " or urg_name like '%" + args.urgency[x] + "%'"
                where_list.append(str_urg)

            if args.status is not None:
                str_urg = ""
                for x in range(0, len(args.status) - 1):
                    str_urg += "'" + args.status[x] + "'" + ","
                str_urg += "'" + args.status[-1] + "'"
                where_list.append(f"st_name in ({str_urg})")

            if args.severity is not None:
                str_urg = ""
                str_urg += " severity like '" + args.severity[0] + "%'"
                if len(args.severity) > 1:
                    for x in range(1, len(args.severity) - 1):
                        str_urg += " or severity like '" + args.severity[x] + "%'"
                where_list.append(str_urg)

            if args.fdate is not None and args.sdate is not None:
                fdate = "".join(x + " " for x in args.fdate)
                fdate = re.sub(r' ', '', fdate)
                sdate = "".join(x + " " for x in args.sdate)
                sdate = re.sub(r' ', '', sdate)
                where_list.append(f"date_discovered between '{fdate}' and '{sdate}'")
            if args.fdate is not None and args.sdate is None:
                fdate = "".join(x + " " for x in args.fdate)
                fdate = re.sub(r' ', '', fdate)
                where_list.append(f"date_discovered >='{fdate}' ")
            if args.sdate is not None and args.fdate is None:
                sdate = "".join(x + " " for x in args.sdate)
                sdate = re.sub(r' ', '', sdate)
                where_list.append(f"date_discovered <='{sdate}' ")
            if len(where_list) != 0:
                where += where_list[0]
                for param in range(1, len(where_list)):
                    where += " and " + where_list[param]
                sql += f" {where}"

            self.query = sql
            print(self.query)
        except Exception as e:
            self._error(e)

    def run(self, cve):
        try:
            print(cve)
            if cve.assm_id is not None and not cve.joint:
                self.assm_vul(cve)
            if cve.pkg_vrs_id is not None:
                self.get_pkg_cve(cve)
            if cve.joint:
                if cve.assm_id is not None:
                    if cve.delete:
                        self.delete_table("vul_assm")
                    else:
                        self.joint_assm_vul(cve)
                else:
                    self._error(" assm_id don't null")
            self.run_query(build=False, t_id=False)
            return self.tbl_dict
        except Exception as e:
            self._error(e)
